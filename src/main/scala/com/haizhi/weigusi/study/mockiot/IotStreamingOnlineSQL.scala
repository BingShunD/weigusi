package com.haizhi.weigusi.study.mockiot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, LongType}

object IotStreamingOnlineSQL {
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

    selectDf.createOrReplaceTempView("selectView")

    val resDf = spark.sql(
      """
        |select device_type,count(device_type) as count_device,round(avg(signal),2) as avg_signal
        | from selectView where signal > 30 group by device_type
        |""".stripMargin)

    val query = resDf.writeStream
      .outputMode(OutputMode.Complete())
      .foreachBatch((df: DataFrame, batchId: Long) => {
        println("~~~~~~~")
        println(s"batchId is $batchId")
        println("~~~~~~~")
        if (!df.isEmpty) {
          df.coalesce(1).show(20, false)
        }
      }).start()
    query.awaitTermination()
    query.stop()
  }
}
