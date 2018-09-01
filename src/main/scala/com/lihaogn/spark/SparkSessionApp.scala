package com.lihaogn.spark

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("/Users/Mac/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")

    people.show()

    spark.stop()
  }
}
