// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

def increment_id(df: DataFrame, amount: Int): DataFrame = {
  df.withColumn("id", $"id" + amount)
}

// COMMAND ----------

def remove_letter_o(df: DataFrame): DataFrame = {
  df.withColumn("data", regexp_replace($"data", "o", ""))
}

// COMMAND ----------

def transform(df: DataFrame, amount: Int): DataFrame = {
  val df_increment = increment_id(df, amount)
  val df_removed = remove_letter_o(df_increment)
  return df_removed
}
