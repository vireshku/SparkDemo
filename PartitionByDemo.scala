package com.infy.demo.entry

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object PartitionByDemo {
  
  


  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/zipcodes.csv")

    val rdd2:RDD[Array[String]] = rdd.map(m=>m.split(","))


    val rdd3 = rdd2.map(a=>(a(1),a.mkString(",")))

    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))

    rdd4.saveAsTextFile("c:/tmp/output/partition2")


  }

  
}