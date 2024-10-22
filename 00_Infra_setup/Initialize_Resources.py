# Databricks notebook source
# MAGIC %md
# MAGIC # Airbnb Model Serving: Initialization Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Catalog, Schema, Volume

# COMMAND ----------

_catalog = 'users'
_schema = 'gabriele_albini'
_volume = 'airbnb'

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+'.'+_schema)

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+'.'+_schema+'.'+_volume)
