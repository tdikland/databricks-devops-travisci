# Databricks notebook source
query = """SELECT * FROM my_table WHERE cond = 'foo';"""

# COMMAND ----------

def select_foo():
  spark.sql(query)
