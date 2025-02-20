# Databricks notebook source
# MAGIC %md 
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md 
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Fetch Relative Columns

# COMMAND ----------

df_source = spark.sql('''
select DISTINCT(Date_ID) as Date_ID 
from parquet.`abfss://silver@cardatalakeritesh.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_source.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Dim Model Sink Initital and Incremental (Just Bring the Schema if table does not exist)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model_date'):
    
    df_sink = spark.sql('''SELECT dim_date_key, Date_ID
    from cars_catalog.gold.dim_date
    ''')
else:
    
    df_sink = spark.sql('''SELECT 1 as dim_date_key, Date_ID
    from parquet.`abfss://silver@cardatalakeritesh.dfs.core.windows.net/carsales`
    where 1=0
    ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Records and Old Records

# COMMAND ----------

df_filter = df_source.join(df_sink, df_source['Date_ID'] == df_sink['Date_ID'], 'left').select (df_source['Date_ID'], df_sink['dim_date_key']) 

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old 

# COMMAND ----------

df_filter.old = df_filter.filter(df_filter['dim_date_key'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(
    df_filter['dim_date_key'].isNull()
).select(
    df_source['Date_ID']
)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key

# COMMAND ----------

# MAGIC %md 
# MAGIC ## FETCH THE MAX SURROGATE KEY FROM EXISTING TABLE
# MAGIC

# COMMAND ----------

if (incremental_flag == '0'):
  max_value = 1
else:
  max_value_df = spark.sql("SELECT  max(dim_date_key) FROM cars_catalog.gold.dim_date")    
  max_value = max_value_df.collect()[0][0]  

# COMMAND ----------

# MAGIC %md 
# MAGIC ### CREATE SURROGATE KEY COLUMN AND ADD THE MAX SURROGATE KEY

# COMMAND ----------

from pyspark.sql.functions import *

df_filter_new = df_filter_new.withColumn('dim_date_key', max_value+ monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### CREATE FINAL DF- df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter.old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # SCD Type -1 (UPSERT)

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

from delta.tables import DeltaTable

# Upsert means Update + Insert

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@cardatalakeritesh.dfs.core.windows.net/dim_date")

    delta_tbl.alias("trg").merge(
        df_final.alias("src"),
        "trg.dim_date_key = src.dim_date_key"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

# Initial run    
else: 
    df_final.write.format('delta')\
                .mode('overwrite')\
                .option("path", "abfss://gold@cardatalakeritesh.dfs.core.windows.net/dim_date")\
                .saveAsTable("cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from cars_catalog.gold.dim_date