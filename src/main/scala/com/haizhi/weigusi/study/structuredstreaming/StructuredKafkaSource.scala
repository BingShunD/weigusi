package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object StructuredKafkaSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "WordCount")
      .option("maxOffsetsPerTrigger", 100000)
      .load()

    import spark.implicits._
    val resDf = inputDf.selectExpr("cast(value as string)")
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .groupBy($"value")
      .count()

    val query = resDf.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    query.awaitTermination()
    query.stop()
  }
}
