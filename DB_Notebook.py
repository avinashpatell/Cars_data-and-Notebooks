# Databricks notebook source
# MAGIC %md
# MAGIC #Create Cataog 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema IF NOT EXISTS cars_catalog.gold;

# COMMAND ----------

