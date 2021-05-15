package com.infy.demo.entry

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Avro1 {

  def main(args: Array[String]): Unit = {
    
    
     val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(
      ("James ", "", "Smith", 2018, 1, "M", 3000),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      ("Jen", "Mary", "Brown", 2010, 7, "", -1))

    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")
      
      
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)
    
    df.show()
    
     /**
     * Write Avro File
     */
    df.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("C:\\tmp\\spark_out\\avro\\person.avro")
      
    
    println("Data write finished........")
    
        /**
      * Read Avro File
      */
    spark.read.format("avro").load("C:\\tmp\\spark_out\\avro\\person.avro").show()
    
      /**
      * Write Avro Partition
      */
    df.write.partitionBy("dob_year","dob_month")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save("C:\\tmp\\spark_out\\avro\\person_partition.avro")

    /**
      * Reading Avro Partition
      */
    spark.read
      .format("avro")
      .load("C:\\tmp\\spark_out\\avro\\person_partition.avro")
      .where(col("dob_year") === 2010)
      .show()



  }

}