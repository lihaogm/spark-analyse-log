package com.lihaogn.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame与RDD互操作
  */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 反射方式
//    inferReflection(spark)

    // 编程方式
    program(spark)

    spark.stop()
  }

  def program(spark:SparkSession): Unit ={

    // RDD ==> DataFrame
    val rdd=spark.sparkContext.textFile("/Users/Mac/testdata/infos.txt")

    val infoRDD=rdd.map(_.split(",")).map(line=>Row(line(0).toInt,line(1),line(2).toInt))

    val structType=StructType(Array(StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)))

    val infoDF=spark.createDataFrame(infoRDD,structType)
    infoDF.printSchema()
    infoDF.show()

    // 通过df的api操作
    infoDF.filter(infoDF.col("age")>30).show()

    // 通过sql方式操作
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age>30").show()
  }


  def inferReflection(spark:SparkSession): Unit ={

    // RDD ==> DataFrame
    val rdd=spark.sparkContext.textFile("/Users/Mac/testdata/infos.txt")

    // 需要导入隐式转换
    import spark.implicits._
    val infoDF=rdd.map(_.split(",")).map(line=>Info(line(0).toInt,line(1),line(2).toInt)).toDF()

    infoDF.show()

    infoDF.filter(infoDF.col("age")>30).show()

    // sql方式
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age>30").show()
  }

  case class Info(id: Int, name: String, age: Int)

}
