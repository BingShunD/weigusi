package com.haizhi.weigusi.study.structuredstreaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StructuredFileSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("duan-structured streaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val csvScheme: StructType = StructType(Seq(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("hobby", StringType, true)
    ))

    //    val structType: StructType = new StructType()
    //      .add(StructField("name", StringType, false))
    //      .add(StructField("age", IntegerType, false))
    //      .add(StructField("hobby", StringType, false))


    val inputDf: DataFrame = spark.readStream
      .schema(csvScheme)
      .option("sep", ";")
      .option("header", "false")
      .csv("data/csvfile/")

    import spark.implicits._

    val resDf = inputDf
      .filter($"age" < 25)
      .groupBy($"hobby")
      .count()
      .orderBy($"count".desc)

    val query: StreamingQuery = resDf.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()
  }
}
