# Databricks notebook source
# MAGIC %md
# MAGIC # DATA Reading

# COMMAND ----------

df = spark.read.format('parquet') \
                .option('inferSchema', 'true')\
                .load('abfss://bronze@carstorageacc.dfs.core.windows.net/raw_data/')

# COMMAND ----------

df.display()


# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumn('Model_Category',split(col('Model_ID'), '-')[0])
display(df)

# COMMAND ----------

df = df.select('Branch_ID', 'Dealer_ID', 'Model_ID', 'Model_Category', 'Revenue', 'Units_Sold', 'Date_ID', 'Day', 'Month', 'Year', 'BranchName', 'DealerName', 'Product_Name')
display(df)

# COMMAND ----------

df = df.withColumn('revenuePerUnit', col('Revenue') / col('Units_Sold'))


df = df.select('Branch_ID', 'Dealer_ID', 'Model_ID', 'Model_Category', 'Revenue', 'Units_Sold', 'revenuePerUnit', 'Date_ID', 'Day', 'Month', 'Year', 'BranchName', 'DealerName', 'Product_Name')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # ad-hoc

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('units_sold').alias('total_units_sold')).sort('Year','total_units_sold', ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@carstorageacc.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`