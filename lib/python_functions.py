from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def increment_id(df: DataFrame, amount: int) -> DataFrame:
  return df.withColumn("id", F.col("id") + amount)

def remove_letter_o(df: DataFrame) -> DataFrame:
  return df.withColumn("data", F.regexp_replace("data", "o", ""))