package com.haizhi.weigusi.study.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

object SparkAggTest {
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
    import org.apache.spark.sql.functions._
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
    selectDf.show(10,false)

    selectDf.createOrReplaceTempView("selectView")

    val resDf = spark.sql(
      """
        |select device_type,count(device_type) as count_device,round(avg(signal),2) as avg_signal
        | from selectView where signal > 30 group by device_type
        |""".stripMargin)


//    val resDf = selectDf.filter($"signal" > 30)
//      .groupBy($"device_type")
//      .agg(
//        count($"device_type").as("count_device"),
//        round(avg($"signal"),2).as("avg_signal")
//      )


    resDf.printSchema()
    resDf.show(10,false)
  }
}
