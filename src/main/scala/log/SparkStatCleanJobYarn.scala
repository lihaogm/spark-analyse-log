package log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark清洗操作运行在yarn上
  */
object SparkStatCleanJobYarn {

  def main(args: Array[String]): Unit = {

    if (args.length!=2) {
      println("usage: sparkstatcleanjobyarn <inputpath> <outputpath>")
      System.exit(1)
    }

    val Array(inputPath,outputPath)=args

    val spark=SparkSession.builder().getOrCreate()

    val accessRDD=spark.sparkContext.textFile(inputPath)

    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save(outputPath)

    spark.stop()

  }
}
