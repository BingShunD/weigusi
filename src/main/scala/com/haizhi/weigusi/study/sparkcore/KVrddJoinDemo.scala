package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object KVrddJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("duan-spark").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val money = Array(
      ("zhangsan", 100), ("lisi", 100), ("wangwu", 200), ("zhaoliu", 600), ("maqi", 90), ("sunjiu", 90)
    )
    val mRdd = sc.parallelize(money, 2)
    val work = Array(
      ("zhangsan", "tech"), ("zhaoliu", "boss"),("qiyu","caoren")
    )
    val wRdd = sc.parallelize(work, 2)

    println("===Join===")
    //join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
    val resRdd: RDD[(String, (Int, String))] = mRdd.join(wRdd)
    resRdd.foreach(println)

    println("===leftOuterJoin===")
    //leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
    val resRdd1 = mRdd.leftOuterJoin(wRdd)
    resRdd1.foreach(println)

    println("===fullOuterJoin===")
    mRdd.fullOuterJoin(wRdd).foreach(println)

    println("===cartesian===")
    mRdd.cartesian(wRdd).foreach(println)

    Thread.sleep(100000000)
    sc.stop()
  }
}
