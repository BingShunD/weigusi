package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object ReduceDemo {
  //rdd中的数相加，也可以使用fold，fold可以定义初始值
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(1 to 10, 2)
    val i = rdd.reduce((tmp, item) => {
      println(s"p-${TaskContext.getPartitionId()}:tmp=${tmp},item=${item},sum=${tmp + item}")
      tmp + item
    })
    println(s"res=$i")
  }
}
