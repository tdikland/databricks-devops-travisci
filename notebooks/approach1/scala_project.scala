// Databricks notebook source
// setup widgets, useful for parameterization of differences between dev/test/prod.
// widgets are structured as: `name`, `default value` and `label`.
dbutils.widgets.text("source_table", "bronze.default", "source table name")
dbutils.widgets.text("target_table", "silver.default", "target table name")
dbutils.widgets.text("increment_amt", "1", "Increment amount for id column")

// COMMAND ----------

// read current widget values.
// in development: values can be manually set on top of the notebook
// in testing: values will be passed from the testing notebook
// in prod: values will be passed from orchestration (airflow)
val source_table = dbutils.widgets.get("source_table")
val target_table = dbutils.widgets.get("target_table")
val increment_amount = dbutils.widgets.get("increment_amt").toInt

// COMMAND ----------

// Assume that the workflow is not modular at all.
// This makes it difficult to run unit tests or do coverage analysis.
// However, it is possible to do an "integration/data" test (if transformations are deterministic). 
// This is done by crafting a source table and an expected result
val source_df = spark.read.table(source_table)
val transformed_df = source_df.withColumn("id", $"id" + increment_amount)
transformed_df.write.mode("overwrite").saveAsTable(target_table)
