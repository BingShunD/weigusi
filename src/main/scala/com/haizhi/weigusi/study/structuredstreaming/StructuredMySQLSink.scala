package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

object StructuredMySQLSink {
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

    //df转ds这里需要隐式转换
    import spark.implicits._
    val resDf = inputDf
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .groupBy($"value")
      .count()

    val query: StreamingQuery = resDf.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-socket-wc")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreach(new MySQLForeachWriter)
      .option("checkpointLocation","data/structured/ckp005")
      .start()

    query.awaitTermination()
    query.stop()
  }
}
