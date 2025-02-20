# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # create flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # creating dimension model
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch dim_model columns

# COMMAND ----------

df_src = spark.sql('''
            select distinct(dealer_id), DealerName 
            from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`
''')

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_branch_sink - initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('''
              select dim_dealer_key, dealer_id as dealer_id, DealerName
              from cars_catalog.gold.dim_dealer
              ''')
else:
     df_sink = spark.sql('''
              select 1 as dim_dealer_key, dealer_id as dealer_id, DealerName
              from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`
              where 1 = 0
              ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.dealer_id == df_sink.dealer_id, 'left').select(df_src.dealer_id, df_src.DealerName, df_sink.dim_dealer_key)
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_dealer_key.isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_dealer_key.isNull()).select(df_src.dealer_id, df_src.DealerName)
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC ***fetch the max surrogate key from existing table***

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql('''
                          select max(dim_dealer_key) from cars_catalog.gold.dim_dealer''')
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create final df - df_filter_new + df_filter old

# COMMAND ----------

final_df = df_filter_old.union(df_filter_new)
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD(slowly changing dimensions) type-1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable 

# COMMAND ----------

from delta.tables import DeltaTable

df_final = spark.createDataFrame([], schema='dim_dealer_key STRING, dealer_id STRING')  # incremental_run

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@carstorageacc.dfs.core.windows.net/dim_dealer")
    delta_table.alias('target').merge(
        df_final.alias('source'),
        'target.dim_dealer_key = source.dim_dealer_key'
    ).whenMatchedUpdate(
        set={
            "dim_dealer_key": "source.dim_dealer_key",
            "dealer_id": "source.dealer_id"
        }
    ).whenNotMatchedInsert(
        values={
            "dim_dealer_key": "source.dim_dealer_key",
            "dealer_id": "source.dealer_id"
        }
    ).execute()

# initial RUN
else:
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path', "abfss://gold@carstorageacc.dfs.core.windows.net/dim_dealer")\
            .saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer