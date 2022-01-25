package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object RDDParallelizeTest {
  def getParValue(idx: Int, ds: Iterator[String], currentPar: Int): Iterator[String] = {
    if (currentPar == idx) ds else Iterator()
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    val seq = Seq(
      "hadoop scala hive spark scala sql sql",
      "hadoop scala spark hdfs hive spark",
      "spark hdfs spark duan scala hive spark",
      "spark hdfs spark bing scala hive spark",
      "spark hdfs spark shun scala hive spark"
    )
    val value2: RDD[String] = sc.parallelize(seq, 3)
    val rdd = sc.makeRDD(seq, 3)

//    val value2 = rdd.mapPartitionsWithIndex((index, dates) => {
//      dates.map((_, s"分区号:$index"))
//    })
//    value2.foreach(println)


    //mapPartitionsWithIndex用于获取当前分区的rdd数据很方便
    for (partition <- rdd.partitions) {
      val value = rdd.mapPartitionsWithIndex(getParValue(_, _, partition.index), true)
      println(partition.index+":index")
      val value1 = value.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
      value1.foreach(println)
    }


//    val rdd1 = rdd.flatMap(_.split("\\s+"))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//    rdd1.foreach(println)
  }
}
