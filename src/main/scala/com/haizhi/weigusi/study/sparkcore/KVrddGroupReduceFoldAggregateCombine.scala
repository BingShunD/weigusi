package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.sql.SparkSession

object KVrddGroupReduceFoldAggregateCombine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("duan-spark").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val strings = Seq(
      "hadoop scala hive spark scala sql sql",
      "hadoop scala spark hdfs hive spark",
      "spark hdfs spark duan scala hive spark",
      "spark hdfs spark bing scala hive spark",
      "spark hdfs spark shun scala hive spark"
    )
    val rdd = sc.parallelize(strings, 2)
    val resRdd = rdd.flatMap(_.trim.split("\\s+"))
      .map((_, 1))

    println("===groupBykey===")
    resRdd.groupByKey().foreach(x => {
      println((x._1, x._2.sum))
    })

    println("===reduceBykey===")
    resRdd.reduceByKey(_ + _).foreach(println)

    println("===foldBykey===")
    resRdd.foldByKey(0)(_ + _).foreach(println)

    println("===aggregateBykey===")
    /*
    aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
    combOp: (U, U) => U): RDD[(K, U)]
    */
    resRdd.aggregateByKey(0)(
      (tmp, item) => {
        tmp + item
      },
      (tmp, res) => {
        tmp + res
      }
    ).collectAsMap().foreach(println)

    println("===combineBykey===")
    /*
    combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    numPartitions: Int): RDD[(K, C)]
    */
    resRdd.combineByKey(
      (value) => value,
      (comb: Int, value) => {
        comb + value
      },
      (comb1: Int, comb2: Int) => {
        comb1 + comb2
      }
    ).foreach(println)

    println("===get avg by combineBykey===")
    val datas = Seq(
      ("xiaoming", "Math", 98),
      ("xiaoming", "English", 88),
      ("wangwu", "Math", 75),
      ("wangwu", "English", 78),
      ("lihua", "Math", 90),
      ("lihua", "English", 80),
      ("zhangsan", "Math", 91),
      ("zhangsan", "English", 80)
    )

    //("xiaoming", "Math", 98) => ("xiaoming",("xiaoming", "Math", 98))
    val kvDatas = for (elem <- datas) yield {
      (elem._1, elem)
    }

    sc.parallelize(kvDatas)
      //返回的RDD[(K, C)]为key+combine的二元组
      .combineByKey(
        //创建combine，x为kvDatas的值，如("xiaoming", "Math", 98) => (98,1),rdd中的值拿过来变成(分数，次数)
        x => (x._3, 1),
        //combine和kvDatas的值做计算,(98,1)与("xiaoming", "English", 88),(98+88,1+1)
        (comb: (Int, Int), value) => (comb._1 + value._3, comb._2 + 1),
        //combine和combine做计算
        (comb1: (Int, Int), comb2: (Int, Int)) => (comb1._1 + comb2._1, comb1._2 + comb2._2)
      )
      .foreach(x =>
        println((x._1, x._2._1 / x._2._2))
      )

    Thread.sleep(1000000)
    sc.stop()
  }
}
