package com.lihaogn.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用
  * 使用时需要通过 --jars 将MySQL驱动包传递到classpath
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {

    // 1) 创建相应的Context
    val sparkConf =new SparkConf()

//    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc=new SparkContext(sparkConf)

    val hiveContext=new HiveContext(sc)

    // 2) 相关的处理：表
    hiveContext.table("student").show

    // 3) 关闭资源
    sc.stop()
  }
}
