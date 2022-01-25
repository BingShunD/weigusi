package com.haizhi.weigusi.study.structuredstreamingdemo

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkBatchKafka {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "etlStation2")
      .option("startingOffset", "earliest")
      .option("endingOffset", "latest")
      .load()

    println(s"inputDfCount=${inputDf.count()}")
    inputDf.printSchema()
    import spark.implicits._
    inputDf.selectExpr("cast(key as string)", "cast(value as string)")
      .as[(String, String)]
      .show(10, false)

    import org.apache.spark.sql.functions._
    inputDf
      .selectExpr(
        "topic", "partition", "offset", "timestamp", "timestampType",
        "cast(key as string)", "cast(value as string)"
      )
      .filter($"value".isNotNull && length(trim($"value")).gt(0))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("output/log3")

    spark.stop()
  }
}
