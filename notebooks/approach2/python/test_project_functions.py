# Databricks notebook source
# MAGIC %run ./project_functions

# COMMAND ----------

from runtime.nutterfixture import NutterFixture, tag
from chispa.dataframe_comparer import *

class MyPythonProjectFunctionsTest(NutterFixture):
  def assertion_python_increment_id(self):
    df_in = spark.createDataFrame([(0, "hello"), (-200, "world"), (200, "foo")], "id INT, data STRING")
    df_out = increment_id(df_in, 8)
    df_exp = spark.createDataFrame([(8, "hello"), (-192, "world"), (208, "foo")], "id INT, data STRING")
    assert_df_equality(df_out, df_exp, ignore_row_order=True)

  def assertion_python_increment_id_negative(self):
    df_in = spark.createDataFrame([(0, "hello"), (-200, "world"), (200, "foo")], "id INT, data STRING")
    df_out = increment_id(df_in, -8)
    df_exp = spark.createDataFrame([(-8, "hello"), (-208, "world"), (192, "foo")], "id INT, data STRING")
    assert_df_equality(df_out, df_exp, ignore_row_order=True)

  def assertion_python_remove_letter_o(self):
    df_in = spark.createDataFrame([(0, "hello"), (-200, "world")], "id INT, data STRING")
    df_out = remove_letter_o(df_in)
    df_exp = spark.createDataFrame([(0, "hell"), (-200, "wrld")], "id INT, data STRING")
    assert_df_equality(df_out, df_exp, ignore_row_order=True)

  def assertion_python_remove_letter_o_consecutive(self):
    df_in = spark.createDataFrame([(0, "foo")], "id INT, data STRING")
    df_out = remove_letter_o(df_in)
    df_exp = spark.createDataFrame([(0, "f")], "id INT, data STRING")
    assert_df_equality(df_out, df_exp, ignore_row_order=True)
    
    
result = MyPythonProjectFunctionsTest().execute_tests()
print(result.to_string())

# check if it is a local run or a job run. 
# `result.exit(dbutils)` should not run in locally as this prevent the display of test results.
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)
