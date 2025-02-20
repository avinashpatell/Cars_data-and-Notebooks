# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql('''select * from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`''')
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all the Dimensions**

# COMMAND ----------

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")
df_dealer.display()

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing Keys to the Fact Table**

# COMMAND ----------

df_fact = df_silver.join(df_dealer, df_silver['dealer_id'] == df_dealer.dealer_id, 'left')\
                   .join(df_date, df_silver['date_id'] == df_date.date_id, 'left')\
                   .join(df_model, df_silver['model_id'] == df_model.model_id, 'left')\
                   .join(df_branch, df_silver['branch_id'] == df_branch.branch_id, 'left')\
                    .select(df_silver['Revenue'], df_silver['Units_Sold'], df_silver['revenuePerUnit'], df_branch['dim_branch_key'], df_dealer['dim_dealer_key'], df_model['dim_model_key'],  df_date['dim_date_key'])

df_fact.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Writing Fact Table**

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.fact_sales'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@carstorageacc.dfs.core.windows.net/factsales')

    delta_table.alias('target').merge(df_fact.alias('source'), 'target.dim_branch_key = source.dim_branch_key and target.dim_dealer_key = source.dim_dealer_key and target.dim_model_key = source.dim_model_key and target.dim_date_key = source.dim_date_key')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:
    df_fact.write.format('delta')\
            .mode('Overwrite')\
            .option('path','abfss://gold@carstorageacc.dfs.core.windows.net/factsales')\
            .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales