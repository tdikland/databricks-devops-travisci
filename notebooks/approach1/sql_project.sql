-- Databricks notebook source
-- setup widgets, useful for parameterization of differences between dev/test/prod.
-- widgets are structured as: CREATE WIDGET <WIDGET-TYPE> <WIDGET-NAME> DEFAULT <DEFAULT-VALUE>;
CREATE WIDGET TEXT source_table DEFAULT "bronze.default";
CREATE WIDGET TEXT target_table DEFAULT "silve.default";
CREATE WIDGET TEXT increment_amt DEFAULT "1"

-- COMMAND ----------

CREATE
OR REPLACE TABLE $target_table AS
SELECT
  id + ($increment_amt) AS id,
  data
FROM
  $source_table;
