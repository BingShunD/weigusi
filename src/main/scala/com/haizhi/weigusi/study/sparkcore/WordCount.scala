package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    //wholeTextFiles读取文件夹  小问价适用
    val rdd: RDD[String] = sc.textFile("conf/wordtest",3)
    println("rdd.getNumPartitions="+rdd.getNumPartitions)
    println("rdd.partitions.length="+rdd.partitions.length)
    //    rdd.foreach(println)
    //    println("~~~~~~~~~~~~~~")
    //val value: RDD[String] = rdd.flatMap(line => line.split("//s+"))
    val rdd1: RDD[(String, Int)] = rdd.flatMap(line => line.split("\\s+"))
      //    rdd1.foreach(println)
      .filter(_!="hadoop")
      .map(word => (word,1))
      .reduceByKey(_+_)
    //sc.runJob   action函数会调用sc.runjob触发job
    rdd1.foreach(println)

    rdd1.coalesce(1).saveAsTextFile("conf/wordtestCOUNT")

    Thread.sleep(10000000)
    sc.stop()
  }
}
