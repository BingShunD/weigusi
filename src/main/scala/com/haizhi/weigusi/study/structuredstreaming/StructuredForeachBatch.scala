package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StructuredForeachBatch {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val processDf = inputDf
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .groupBy($"value")
      .count()

    val query = processDf.writeStream
      .outputMode(OutputMode.Complete())
      .queryName("海森堡")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "data/structured/ckp007")
      .foreachBatch((batchDf: DataFrame, batchId: Long) => {
        println(s"batchId=$batchId")
        batchDf
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("jdbc")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "duanbs.tb_word_count1")
          .save()
      })
      .start()

    query.awaitTermination()
    query.stop()

  }
}
