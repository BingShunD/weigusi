package com.haizhi.weigusi

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSQLEnv {
  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  val stopped: AtomicBoolean = new AtomicBoolean(false)

  def init() = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("hive_to_hive")
      // 设置输出文件算法
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")

    val builder: SparkSession.Builder = SparkSession.builder()
      .config(sparkConf)

    builder
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")

    sparkSession = builder.getOrCreate()
    sparkContext = sparkSession.sparkContext
  }

  def stop(): Unit ={
    stopped.getAndSet(true)
    if(sparkContext != null && !sparkContext.isStopped){
      sparkContext.stop()
    }
  }
}
