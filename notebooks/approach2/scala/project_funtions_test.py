# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag

class MyScalaProjectFunctionsTest(NutterFixture):  
  def run_scala_function_test(self):
    self.test_status = dbutils.notebook.run("project_functions_unit", 600)
    
  def assertion_scala_function_test(self):
    assert self.test_status == "TEST SUCCEEDED"
    
    
result = MyScalaProjectFunctionsTest().execute_tests()
print(result.to_string())

# check if it is a local run or a job run. 
# `result.exit(dbutils)` should not run in locally as this prevent the display of test results.
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)

# COMMAND ----------


