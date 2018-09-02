package log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 统计spark作业
  */
object TopNStatJobYARN {

  /**
    * 最受欢迎的topN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    /**
      * 使用DataFrame方式统计
      */
    import spark.implicits._

    val videoAccessTopDF = accessDF.filter($"day" === day && $"classType" === "video")
      .groupBy("day", "classId").agg(count("classId").as("times")).orderBy($"times".desc)

    //    videoAccessTopDF.show(false)

    /**
      * 使用SQL方式进行统计
      */
    //    accessDF.createOrReplaceTempView("access_logs")
    //    val videoAccessTopDF=spark.sql("select day,classId, count(1) as times from access_logs "+
    //      "where day=" + day + " and classType='video' " +
    //      "group by day,classId order by times desc")
    //
    //    videoAccessTopDF.show(false)

    /**
      * 将统计结果写入mysql
      */

    try {
      videoAccessTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVedioAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val times = info.getAs[Long]("times")

          list.append(DayVedioAccessStat(day, classId, times))
        })

        // 将数据插入数据库
        StatDAO.insertDayVedioAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }

  /**
    * 按地市统计topN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    * @return
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    import spark.implicits._

    val cityAccessTopDF = accessDF.filter($"day" === day && $"classType" === "video")
      .groupBy("day", "city", "classId").agg(count("classId").as("times"))

    //    cityAccessTopDF.show(false)

    // window函数在spark sql的使用
    val top3DF = cityAccessTopDF.select(
      cityAccessTopDF("day"),
      cityAccessTopDF("city"),
      cityAccessTopDF("classId"),
      cityAccessTopDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopDF("city"))
        .orderBy(cityAccessTopDF("times").desc))
        .as("times_rank")
    ).filter("times_rank <= 3")

    /**
      * 将统计结果写入mysql
      */

    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, classId, city, times, timesRank))
        })

        // 将数据插入数据库
        StatDAO.insertDayCityVedioAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按流量统计topN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    * @return
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    import spark.implicits._

    val videoAccessTopDF = accessDF.filter($"day" === day && $"classType" === "video")
      .groupBy("day", "classId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)

    //    videoAccessTopDF.show(false)

    /**
      * 将统计结果写入mysql
      */
    try {
      videoAccessTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayTrafficVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayTrafficVideoAccessStat(day, classId, traffics))
        })

        // 将数据插入数据库
        StatDAO.insertDayVedioTrafficsAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("usage: topnstatjobyarn <inputpath> <day>")
      System.exit(1)
    }

    val Array(inputPath, day) = args

    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()

    val accessDF = spark.read.format("parquet").load(inputPath)

    StatDAO.deleteData(day)

    // 最受欢迎的topN课程
    videoAccessTopNStat(spark, accessDF, day)

    // 按地市统计topN课程
    cityAccessTopNStat(spark, accessDF, day)

    // 按流量统计topN课程
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()


  }


}
