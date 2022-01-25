package com.haizhi.weigusi.study.structuredstreamingdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object StructuredEtlSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stationTopic")
      .load()

    import spark.implicits._
    //station_9,18600004601,18900009574,success,1641785450649,5000
    val etlDf = inputDf.selectExpr("cast(key as string)","cast(value as string)")
      .as[(String,String)]
      .filter(line => null != line._2 && line._2.trim.split(",").length == 6 && "success".equals(line._2.trim.split(",")(3)))

    etlDf.printSchema()

    val query = etlDf.writeStream
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "etlStation")
      .option("checkpointLocation","data/structured/ckp001")
      .start()

//    val query = etlDf.writeStream
//      .outputMode(OutputMode.Append())
//      .format("console")
//      .option("numRows", 10)
//      .option("truncate", "false")
//      .start()

    query.awaitTermination()
    query.stop()
  }
}
