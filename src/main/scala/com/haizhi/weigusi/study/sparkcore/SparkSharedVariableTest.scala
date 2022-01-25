package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkSharedVariableTest {
  //broadcast广播变量 accumulator累加器
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("conf/wordall",3)
    val strings = Seq(",", ".", "!", "#", "$", "&")
    val broadcastValue: Broadcast[Seq[String]] = sc.broadcast(strings)
    val accumulator = sc.longAccumulator("My accumulator")

    val resRdd = rdd.filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .filter(word => {
        val isContain = broadcastValue.value.contains(word)
        if (isContain) {
          accumulator.add(1L)
        }
        !isContain
      }
      )
      .mapPartitions(iert => {
        iert.map((_, 1))
      })
      .reduceByKey(_ + _)

    resRdd.foreach(println)
    println("不符合的字符"+accumulator.value)

    Thread.sleep(10000000)
    sc.stop()

  }
}
