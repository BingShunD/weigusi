package com.haizhi.weigusi.study.demo

import com.alibaba.fastjson.JSON
import com.haizhi.weigusi.study.mockiot.DeviceData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkDsTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "iotTopic")
      .option("startingOffset", "earliest")
      .option("endingOffset", "latest")
      .load()

    println(s"inputDfCount=${inputDf.count()}")
    inputDf.printSchema()

    import spark.implicits._
    //import org.apache.spark.sql.functions._

    val selectDs: Dataset[DeviceData] = inputDf.selectExpr("cast(value as string)")
      .as[String]
      .filter(line => null != line && line.length > 0)
      .mapPartitions(iter => {
        iter.map(line =>
          JSON.parseObject(line.trim, classOf[DeviceData])
        )
      })

    selectDs.printSchema()
    selectDs.show(10,false)

//    selectDs
//      .filter($"signal" > 90)
//      .groupBy($"deviceType")
//      .agg(
//        count($"deviceType").as("count_device"),
//        round(avg($"signal"),2).as("avg_signal")
//      )
//      .show(10,false)

    //下面的count、avg用到
    import org.apache.spark.sql.expressions.scalalang.typed._
    val resDf: DataFrame = selectDs.filter(device => device.signal > 90)
      .groupByKey(device => device.deviceType)
      .agg(
        count(device => device.deviceType), avg(device => device.signal)
      )
      .toDF("device_type", "count_device", "avg_signal")

    resDf.show(10,false)
    resDf.printSchema()



  }
}
