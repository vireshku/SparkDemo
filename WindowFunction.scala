package com.infy.demo.entry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunction extends App{
  
  

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)
    )
    val df = simpleData.toDF("employee_name","department","salary")
    df.show()

    val w2 = Window.partitionBy("department").orderBy(col("salary"))
    df.withColumn("row",row_number.over(w2))
      .where($"row" === 1).drop("row")
      .show()

    val w3 = Window.partitionBy("department").orderBy(col("salary").desc)
    df.withColumn("row",row_number.over(w3))
      .where($"row" === 1).drop("row")
      .show()

    //Maximum, Minimum, Average salary for each window
    val w4 = Window.partitionBy("department")
    val aggDF = df.withColumn("row",row_number.over(w3))
      .withColumn("avg", avg(col("salary")).over(w4))
      .withColumn("sum", sum(col("salary")).over(w4))
      .withColumn("min", min(col("salary")).over(w4))
      .withColumn("max", max(col("salary")).over(w4))
      .where(col("row")===1).select("department","avg","sum","min","max")
      .show()
 
}