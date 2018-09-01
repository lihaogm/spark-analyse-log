package com.lihaogn.spark

import org.apache.spark.sql.SparkSession

object ParquetApp {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    // 加载数据
    val userDF=spark.read.format("parquet")
      .load("/Users/Mac/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show()

    userDF.select("name","favorite_color").write.format("json").save("/Users/Mac/testdata/jsonout")

    // sparksql默认读取的的format就是parquet
    spark.read.load("/Users/Mac/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").show()

    // 另外一种加载方式
    spark.read.format("parquet")
      .option("path","/Users/Mac/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
      .load().show()

    spark.stop()


  }

}
