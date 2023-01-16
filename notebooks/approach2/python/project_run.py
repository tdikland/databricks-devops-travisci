# Databricks notebook source
dbutils.widgets.text(name="source_table", defaultValue="bronze.default", label="source table name")
dbutils.widgets.text(name="target_table", defaultValue="silver.default", label="target table name")
dbutils.widgets.text(name="increment_amt", defaultValue="1", label="Increment amount for id column")

# COMMAND ----------

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
increment = dbutils.widgets.get("increment_amt")

# COMMAND ----------

# MAGIC %run ./project_functions

# COMMAND ----------

df_source = spark.read.table(source_table)
df_transformed = transform(df_source, increment)
df_transform.write.mode("overwrite").saveAsTable(target_table)
