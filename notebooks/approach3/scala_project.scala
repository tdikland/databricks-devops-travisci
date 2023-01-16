// Databricks notebook source
dbutils.widgets.text("source_table", "bronze.default", "source table name")
dbutils.widgets.text("target_table", "silver.default", "target table name")
dbutils.widgets.text("increment_amt", "1", "Increment amount for id column")

// COMMAND ----------

val source_table = dbutils.widgets.get("source_table")
val target_table = dbutils.widgets.get("target_table")
val increment_amount = dbutils.widgets.get("increment_amt").toInt

// COMMAND ----------

// MAGIC %run ../../shared/scala_functions

// COMMAND ----------

val source_df = spark.read.table(source_table)
val transformed_df = transform(source_df, increment_amount)
transformed_df.write.mode("overwrite").saveAsTable(target_table)
