package com.infy.demo.entry

import org.apache.spark.sql.SparkSession

object DEMO3 extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("AddingLiteralDemo1")
    .getOrCreate();

  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq(("111", 5, 50000), ("222", 6, 60000), ("333", 7, 60000))

  val df = data.toDF("EmpId", "Experience", "Salary")
  df.show()

  val newdf = df.withColumn("EmpId", struct(lit("1").as("key"), col("EmpId").as("value")))
    .withColumn("Experience", struct(lit("2").as("key"), col("Experience").as("value")))
    .withColumn("Salary", struct(lit("3").as("key"), col("Salary").as("value")))
    .show(false)

  //-------------------------
    
    val ds = df.as[Employee]
  ds.printSchema()

  val newDS = ds.map(rec => {
    (EmpData("1", rec.EmpId), EmpData("2", rec.Experience.toString), EmpData("3", rec.Salary.toString))
  })

  val finalDS = newDS.toDF("EmpId", "Experience", "Salary").as[Employee2]
  finalDS.show(false)
}
