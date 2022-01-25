package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object AggregateDemo {
  //取元素top2
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(1 to 10, 2)

    //aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    val ints = rdd.aggregate(ListBuffer[Int]())(
      (u, t) => {
        println(s"p-${TaskContext.getPartitionId()}:u=${u},t=${t}")
        u += t
        u.sorted.takeRight(2)
      },
      (u1, u2) => {
        println(s"p-${TaskContext.getPartitionId()}:u1=${u1},u2=${u2}")
        u1 ++= u2
        u1.sorted.takeRight(2)
      }
    )

    println("~~~~~~~~~~~~~~~~")
    ints.foreach(println)

    sc.stop()
  }
}
