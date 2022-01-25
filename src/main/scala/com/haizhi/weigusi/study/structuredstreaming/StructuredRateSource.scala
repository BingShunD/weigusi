package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredRateSource {
  //数据源：Rate Source，以每秒指定的行数生成数据，每个输出行包含一个timestamp和value。
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10") // 每秒生成数据数目
      .option("rampUpTime", "0s") // 每条数据生成间隔时间
      .option("numPartitions", "2") // 分区数目
      .load()

    val query: StreamingQuery = inputDf.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()
  }
}
