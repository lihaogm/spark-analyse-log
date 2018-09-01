package log


import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 使用spark完成数据清洗
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("/Users/Mac/testdata/access.log")

    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)

    // coalesce:设置输出文件的个数，默认3个
//    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
//      .partitionBy("day").save("/Users/Mac/testdata/log_clean/")
  }
}
