package com.haizhi.weigusi.study.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 基于Structured Streaming 读取TCP Socket读取数据，事件时间窗口统计词频，将结果打印到控制台
 * TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)，设置水位Watermark时间为10秒
 * 12:00:05 => 11:59:55 => 12:00:00
dog,2019-10-10 12:00:07
owl,2019-10-10 12:00:08

dog,2019-10-10 12:00:14
cat,2019-10-10 12:00:09

cat,2019-10-10 12:00:15
dog,2019-10-10 12:00:08
owl,2019-10-10 12:00:13
owl,2019-10-10 12:00:21

owl,2019-10-10 12:00:17
 */
object StructuredWatermarkUpdate {
  def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .appName("duan-structured streaming")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 2)
        .getOrCreate()

      //read format option load
      val inputDf: DataFrame = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

      import spark.implicits._
      import org.apache.spark.sql.functions._
      //2019-10-12 09:00:07,owl cat
      val resDf = inputDf.as[String]
        .filter(line => null != line && line.trim.length > 0)
        .map(line => {
          val arr = line.split(",")
          (Timestamp.valueOf(arr(1)),arr(0))
        })
        .toDF("insert_timestamp","word")
        .withWatermark("insert_timestamp","10 seconds")
        .groupBy(
          window($"insert_timestamp","10 seconds","5 seconds"),
          $"word"
        ).count()

      resDf.printSchema()

      val query = resDf.writeStream
        .outputMode("update")
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .option("numRows", 100)
        .option("truncate", false)
        .start()
      query.awaitTermination()
      query.stop()
  }
}
