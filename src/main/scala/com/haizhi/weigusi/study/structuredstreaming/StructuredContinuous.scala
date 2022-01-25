package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StructuredContinuous {
  /**
   * 从Spark 2.3版本开始，StructuredStreaming结构化流中添加新流式数据处理方式：Continuous processing
   * 持续流数据处理：当数据一产生就立即处理，类似Storm、Flink框架，延迟性达到100ms以下，目前属于实验开发阶段
   */
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
      .option("topic", "etlStation2")
      .option("checkpointLocation","data/structured/ckp002")
      .trigger(Trigger.Continuous("1 second"))
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
