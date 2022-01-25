package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("conf/wordtest",3)
    val wordAndOne = rdd.flatMap(line => line.trim.split("\\s+"))
      .mapPartitions(iter => {
        iter.map((_, 1))
      })
      .reduceByKey(_ + _)

    wordAndOne.foreachPartition(datas => {
      val i = TaskContext.getPartitionId()
      datas.foreach(date => println(s"分区为:$i,${date._1}=${date._2}"))
    })
//    wordAndOne.foreachPartition(datas => {
//      val i = TaskContext.getPartitionId()
//      datas.foreach{ case (word,count) =>
//        println(s"分区为:$i,$word:$count")}
//    })
    sc.stop()

  }
}
