package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sql.SparkSession

object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext

    val dataRdd = sc.parallelize(Seq(
      "Spark Hadoop Spark Hadoop Hive Spark",
      "spark hadoop spark hadoop hive spark",
      "12qwe 34wqe 56era spark hadoop hive"
    ))
    val int = "12qwe".asInstanceOf[String].charAt(0).toInt
    println(s"int=$int")

    val resRdd = dataRdd.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _,3)

    resRdd.foreachPartition(iter =>
      println(s"${TaskContext.getPartitionId()}+++${iter.mkString(",")}"
      )
    )

    resRdd.partitionBy(new MyPartitioner).foreachPartition(iter => {
      println(s"${TaskContext.getPartitionId()}+++${iter.mkString(",")}")
    })

    dataRdd.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(new MyPartitioner,_ + _).foreachPartition(iter => {
      println(s"${TaskContext.getPartitionId()}+++${iter.mkString(",")}")
    })

    Thread.sleep(1000000)
    sc.stop()

  }
}


class MyPartitioner extends Partitioner{
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    val firstValue = key.asInstanceOf[String].charAt(0).toInt
    if(97 <= firstValue && firstValue <= 122){
      0 // 第一个分区
    }else if(65 <= firstValue && firstValue <= 90){
      1 // 第二个分区
    } else{ 2 // 第三个分区
    }
  }
}