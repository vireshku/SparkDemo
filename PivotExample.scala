package com.infy.demo.entry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
    ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

  import spark.sqlContext.implicits._
  val df = data.toDF("Product", "Amount", "Country")
  df.show()

  //pivot
  val pivotDF = df.groupBy("Product", "Country")
    .sum("Amount")
    .groupBy("Product")
    .pivot("Country")
    .sum("sum(Amount)")
  pivotDF.show()

  val countries = Seq("USA", "China", "Canada", "Mexico")
  val pivotDF2 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
  pivotDF2.show()

  df.select(collect_list(""))

}