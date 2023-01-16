# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag
from chispa.dataframe_comparer import *

class MyScalaProjectTest(NutterFixture):
    def __init__(self):
        # this section defines some general variables that can be used between different tests/phases
        self.source_table_fqn = "tim_dikland.my_source_table"
        self.target_table_fqn = "tim_dikland.my_target_table"
        NutterFixture.__init__(self)
    
    def before_scala_project(self):
        # this section is the "setup" phase. can be used to prepare data, etc.
        spark.sql(f"CREATE OR REPLACE TABLE {self.source_table_fqn} (id INT, data STRING);")
        spark.sql(f"CREATE OR REPLACE TABLE {self.target_table_fqn} (id INT, data STRING);")
        
        source_df = spark.createDataFrame([(0, "hello"), (1, "world")], "id INT, data STRING")
        source_df.write.mode("overwrite").saveAsTable(self.source_table_fqn)
    
    def run_scala_project(self):
        # this is the "run" phase, where the system under test (SUT) is run
        args = {
          "source_table": self.source_table_fqn,
          "target_table": self.target_table_fqn,
          "increment_amt": 7
        }
        dbutils.notebook.run('scala_project', 600, args)

    def assertion_scala_project(self):
        # this phase can be used to verify that the actions in the run phase were properly executed
        res = spark.read.table(self.target_table_fqn)
        exp = spark.createDataFrame([(7, "hello"), (8, "world")], "id INT, data STRING")
        assert_df_equality(res, exp, ignore_row_order=True)
        
    def after_scala_project(self):
        # this section is the "teardown" phase. can be used to release resources, drop data, etc.
        spark.sql(f"DROP TABLE IF EXISTS {self.source_table_fqn};")
        spark.sql(f"DROP TABLE IF EXISTS {self.target_table_fqn};")

result = MyScalaProjectTest().execute_tests()
print(result.to_string())

# check if it is a local run or a job run. 
# `result.exit(dbutils)` should not run in locally as this prevent the display of test results.
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)
