package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}


/*
{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:01:55","eventType": "browse","userID":"1"}
{"eventTime": "2016-01-10 10:01:55","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:02:00","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:01:51","eventType": "click","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"3"}
{"eventTime": "2016-01-10 10:01:51","eventType": "click","userID":"2"}
*/
object StructuredDeduplication {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    //read format option load
    val inputTable: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val resultTable = inputTable
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      // 样本数据：{“eventTime”: “2016-01-10 10:01:50”,“eventType”: “browse”,“userID”:“1”}
      .select(
        get_json_object($"value", "$.eventTime").as("event_time"), //
        get_json_object($"value", "$.eventType").as("event_type"), //
        get_json_object($"value", "$.userID").as("user_id")//
      )
      .dropDuplicates("user_id","event_type")
      .groupBy($"user_id",$"event_type")
      .count()

    resultTable.printSchema()

    val query = resultTable.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", "10")
      .option("truncate", false)
      .start()
    query.awaitTermination()
    query.stop()





  }
}
