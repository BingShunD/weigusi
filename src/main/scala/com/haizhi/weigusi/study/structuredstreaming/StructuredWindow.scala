package com.haizhi.weigusi.study.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredWindow {
  /*
   基于Structured Streaming 模块读取TCP Socket读取数据，进行事件时间窗口统计词频WordCount，将结果打印到控制台
   TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)
   * EventTime即事件真正生成的时间：
   例如一个用户在10：06点击 了一个按钮，记录在系统中为10：06
   这条数据发送到Kafka，又到了Spark Streaming中处理，已经是10：08，这个处理的时间就是process Time。 ** 测试数据：
   2019-10-12 09:00:02,cat dog
   2019-10-12 09:00:03,dog dog
   2019-10-12 09:00:07,owl cat
   2019-10-12 09:00:11,dog
   2019-10-12 09:00:13,owl
   */

/*
  最大的窗口数 = 向上取整(窗口长度/滑动步长) = 10/5
  先计算初始窗口：event-time向上取 能整除 滑动步长的时间) - (最大窗口数×滑动步长)
  9:00:00 - （2 * 5） = 8:59:50
  初始窗口： 8:59:50 ~ 9:00:00：==不包含==

  再按照滑动时间来计算运行窗口
  8:59:55 ~ 9:00:05
  9:00:00 ~ 9:00:10
  9:00:05~ 9:00:15
  9:00:10 ~ 9:00:20

  最后再计算结束窗口
  9:00:15 ~ 9:00:25：如果最大的事件时间不在这个窗口，不再计算，这个窗口作为结束窗口
  ==不包含==
  */

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
      .flatMap(line => {
        val arr = line.trim.split(",")
        arr(1).split("\\s+").map(word => (Timestamp.valueOf(arr(0)), word))
      })
      .toDF("insert_timestamp","word")
      .groupBy(
        window($"insert_timestamp","10 seconds","5 seconds"),
        $"word"
      ).count()
      //.orderBy($"count")
      .orderBy($"window")

    resDf.printSchema()

    val query = resDf.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("numRows", 100)
      .option("truncate", false)
      .start()
    query.awaitTermination()
    query.stop()





  }
}
