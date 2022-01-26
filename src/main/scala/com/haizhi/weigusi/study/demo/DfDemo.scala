package com.haizhi.weigusi.study.demo

import javafx.scene.chart.NumberAxis.DefaultFormatter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
//write方法转成string的时候需要的隐式值DefaultFormats
import org.json4s.{DefaultFormats, JValue}
//write方法
import org.json4s.jackson.Serialization._
//parse方法
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DfDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("hive_to_hive")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")

    val builder: SparkSession.Builder = SparkSession.builder()
      .config(sparkConf)

    builder
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")

    val spark: SparkSession = builder.getOrCreate()

    val inputDf = spark.sql("select * from duanbs.employee")
    inputDf.printSchema()
    inputDf.show(10,false)

    val res: mutable.Seq[Seq[String]] = inputDf.take(5).map(row =>
      row.toSeq.map(d =>
        d.toString
      )
    ).to[ArrayBuffer]


    println("==================")
    implicit val formats = DefaultFormats
    val str: String = write(res)
    val value: JValue = parse(str)
    println(str)
    println("==================")
    println(value)




  }
}
