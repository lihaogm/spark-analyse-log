package com.lihaogn.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext的使用
  */

object SQLContextApp {

  def main(args: Array[String]): Unit = {
    val path=args(0)

    // 1) 创建相应的Context
    val sparkConf =new SparkConf()

    val sc=new SparkContext(sparkConf)

    val sqlContext=new SQLContext(sc)

    // 2) 相关的处理：json
    val people=sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 3) 关闭资源
    sc.stop()
  }
}
