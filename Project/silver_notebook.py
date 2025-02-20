# Databricks notebook source
# MAGIC %md 
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format("parquet")\
            .option('inferSchema', 'true')\
            .load('abfss://bronze@cardatalakeritesh.dfs.core.windows.net/rawdata')


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'), '-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df = df.withColumn('Revperunit',col('Revenue')/col('Units_Sold'))

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # AD-HOC

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(df.groupBy('Year', 'BranchName') .agg(sum('Units_Sold').alias('Total_Units_Sold')) .sort('Year', 'Total_Units_Sold', ascending=[1, 0]))

# COMMAND ----------

# MAGIC %md 
# MAGIC # DATA WRITING

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path', 'abfss://silver@cardatalakeritesh.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from parquet.`abfss://silver@cardatalakeritesh.dfs.core.windows.net/carsales`

# COMMAND ----------

