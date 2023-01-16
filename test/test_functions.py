import pytest

from lib.python_functions import increment_id, remove_letter_o
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture()
def test_data(spark):
  schema = "id INT, data STRING"
  data = [(0, "hello"), (1, "world")]
  return spark.createDataFrame(data, schema)
  
def test_increment_id(spark, test_data):
  result = increment_id(test_data, 7)
  expected = spark.createDataFrame([(7, "hello"), (8, "world")], "id INT, data STRING")
  assert_df_equality(result, expected, ignore_row_order=True)

def test_remove_letter_o(spark, test_data):
  result = remove_letter_o(test_data)
  expected = spark.createDataFrame([(0, "hell"), (1, "wrld")], "id INT, data STRING")
  assert_df_equality(result, expected, ignore_row_order=True)