// Databricks notebook source
// MAGIC %run ./project_functions

// COMMAND ----------

import org.scalatest._
import org.scalatest.events.Event
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, FloatType, StringType}
import scala.collection.JavaConverters._

class MyScalaProjectFunctionsTests extends AsyncFunSuite {
  // Create fake data for the unit tests to run against.
  // In general, it is a best practice to not run unit tests
  // against functions that work with data in production.
  val schema = StructType(Array(
     StructField("id", IntegerType),
     StructField("data", StringType)
  ))
  val data = Seq(
    Row(0, "hello"),
    Row(-200, "world"),
    Row(200, "foo")
  ).asJava
  val df = spark.createDataFrame(data, schema)

  test("test increment_id") {
    val ids = increment_id(df, 4).select("id").collect().toSet;
    assert(ids == Set(Row(4), Row(-196), Row(204)))
  }
  
  test("test increment_id with negative amount") {
    val ids = increment_id(df, -4).select("id").collect().toSet;
    assert(ids == Set(Row(-4), Row(-204), Row(196)))
  }
  
  test("test remove letter o") {
    val ids = remove_letter_o(df).select("data").collect().toSet;
    assert(ids == Set(Row("hell"), Row("wrld"), Row("f")))
  }
}

val suite = new MyScalaProjectFunctionsTests
val reporter = new Reporter() {
  override def apply(e: Event) = {}
}

if (suite.testNames.map(t => suite.run(Some(t), Args(reporter))).map(r => r.succeeds()).contains(false)) {
  dbutils.notebook.exit("TEST FAILURE")
} else {
  dbutils.notebook.exit("TEST SUCCEEDED")
}
