# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace

# COMMAND ----------

def increment_id(df: DataFrame, amount: int) -> DataFrame:
  return df.withColumn("id", col("id") + amount)

# COMMAND ----------

def remove_letter_o(df: DataFrame) -> DataFrame:
  return df.withColumn("data", regexp_replace("data", "o", ""))

# COMMAND ----------

def transform(df: DataFrame, amount: int) -> DataFrame:
  df = increment_id(df, amount)
  df = remove_letter_o(df)
  return df
