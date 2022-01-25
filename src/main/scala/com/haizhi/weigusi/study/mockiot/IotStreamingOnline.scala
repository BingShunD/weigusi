package com.haizhi.weigusi.study.mockiot

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, LongType}

object IotStreamingOnline {
  /**
   * 对物联网设备状态信号数据，实时统计分析: * 1）、信号强度大于30的设备
   * 2）、各种设备类型的数量
   * 3）、各种设备类型的平均信号强度
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("duan-iot")
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "iotTopic")
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", "100000")
      .load()

    //{"device":"device_50","deviceType":"bigdata","signal":35.0,"time":1641891280258}
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val selectDf = inputDf.selectExpr("cast(key as string)", "cast(value as string)")
      .as[(String, String)]
      .filter(tuple => null != tuple._2 && tuple._2.length > 0)
      .select(
        $"key",
        get_json_object($"value", "$.device").as("device"),
        get_json_object($"value", "$.deviceType").as("device_type"),
        get_json_object($"value", "$.signal").cast(DoubleType).as("signal"),
        get_json_object($"value", "$.time").cast(LongType).as("time")
      )
    selectDf.printSchema()

    val resDf = selectDf.filter($"signal" > 30)
      .groupBy($"device_type")
      .agg(
        count($"device_type").as("count_device"),
        round(avg($"signal"),2).as("avg_signal")
      )

    val query = resDf.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "1000")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()






  }
}
