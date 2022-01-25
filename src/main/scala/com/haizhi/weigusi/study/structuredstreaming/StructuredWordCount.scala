package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}

object StructuredWordCount {
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
      .format("console")
      .option("numRows", 10)
      .option("truncate", false)
      .option("checkpointLocation","data/structured/ckp001")
      .start()

/*    while (true){
      println("====================")
      //println("Query Name: "+ query.name)
      println("Query ID: "+ query.id)
      //println("Query RunID: "+ query.runId)
      //println("Query IsActive: "+ query.isActive)
      println("Query Status: "+ query.status)
      println("Query LastProgress: "+ query.lastProgress)
      println("====================")
      Thread.sleep(2 * 1000) }*/

//    spark.streams.addListener(
//      new StreamingQueryListener {
//        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
//          println(s"Query Started:${event.id}")
//        }
//
//        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
//          println(s"===Query made Progress===:${event.progress}")
//        }
//
//        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
//          println(s"Query Terminated:${event.id}")
//        }
//      }
//    )

    //Query Terminated:067c06d9-4e57-47bf-9107-bbd00c8e46f0
    //query.awaitTermination()
    //query.stop()
    StreamingUtils.stopStructuredStreaming(query,"conf/stopfile")

  }
}
