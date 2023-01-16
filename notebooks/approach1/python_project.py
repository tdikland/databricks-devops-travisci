# Databricks notebook source
dbutils.widgets.text(name="source_table", defaultValue="bronze.default", label="source table name")
dbutils.widgets.text(name="target_table", defaultValue="silver.default", label="target table name")
dbutils.widgets.text(name="increment_amt", defaultValue="1", label="Increment amount for id column")

# COMMAND ----------

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
increment = dbutils.widgets.get("increment_amt")

# COMMAND ----------

import pyspark.sql.functions as F

def increment_id(df):
  return df.withColumn("id", F.col("id") + int(increment))

spark.read.table(source_table).transform(increment_id).write.mode("overwrite").saveAsTable(target_table)
