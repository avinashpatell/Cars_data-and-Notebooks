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
            select distinct(model_id), model_category 
            from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`
''')

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model_sink - initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('''
              select dim_model_key, model_id  as model_id, model_category
              from cars_catalog.gold.dim_model
              ''')
else:
     df_sink = spark.sql('''
              select 1 as dim_model_key, model_id  as model_id, model_category
              from parquet.`abfss://silver@carstorageacc.dfs.core.windows.net/carsales`
              where 1 = 0
              ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.model_id == df_sink.model_id, 'left').select(df_src.model_id, df_src.model_category, df_sink.dim_model_key)
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_model_key.isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_model_key.isNull()).select(df_src.model_id, df_src.model_category)
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
                          select max(dim_model_key) from cars_catalog.gold.dim_model''')
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value + monotonically_increasing_id())

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

df_final = spark.createDataFrame([], schema='dim_model_key STRING, model_id STRING')  # incremental_run

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@carstorageacc.dfs.core.windows.net/dim_model")
    delta_table.alias('target').merge(
        df_final.alias('source'),
        'target.dim_model_key = source.dim_model_key'
    ).whenMatchedUpdate(
        set={
            "model_id": "source.model_id"
        }
    ).whenNotMatchedInsert(
        values={
            "dim_model_key": "source.dim_model_key",
            "model_id": "source.model_id"
        }
    ).execute()

# initial RUN
else:
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path', "abfss://gold@carstorageacc.dfs.core.windows.net/dim_model")\
            .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model