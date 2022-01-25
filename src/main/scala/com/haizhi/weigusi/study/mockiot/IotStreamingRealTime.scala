package com.haizhi.weigusi.study.mockiot

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType}

object IotStreamingRealTime {
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
    //import org.apache.spark.sql.functions._
    import spark.implicits._
    val selectDs: Dataset[DeviceData] = inputDf.selectExpr("cast(value as string)")
      .as[String]
      .filter(line => null != line && line.length > 0)
      .mapPartitions(iter => {
        iter.map(line =>
          JSON.parseObject(line.trim, classOf[DeviceData])
        )
      })

    import org.apache.spark.sql.expressions.scalalang.typed._
    val resDf = selectDs.filter(d => d.signal > 10)
      .groupByKey(_.deviceType)
      .agg(
        count(_.device), avg(_.signal)
      )
      .toDF("device_type", "count_device", "avg_signal")

    val query = resDf.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", 10)
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()
  }
}
