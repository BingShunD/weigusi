package com.haizhi.weigusi.study.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object IntersectionUnionSubtractCartesianDemo {
  def main(args: Array[String]): Unit = {
    //交、并、差、笛卡尔积
    val spark = SparkSession.builder().appName("duan-spark").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val ints1 = Seq(1, 2, 3, 4)
    val rdd1 = sc.parallelize(ints1, 2)
    val ints2 = Seq(5, 6, 3, 4)
    val rdd2 = sc.parallelize(ints2, 2)

    println("===intersection===")
    rdd1.intersection(rdd2).foreach(println)

    println("===union===")
    val ints: Array[Int] = rdd1.union(rdd2).distinct().collect()
    ints.foreach(println)

    println("===subtract===")
    rdd1.subtract(rdd2).foreach(println)

    println("===cartesian===")
    val nameRdd = sc.parallelize(Seq("tom", "jack"), 2)
    val learnRdd = sc.parallelize(Seq("java", "scala", "spark"), 2)
    //cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)]
    val value: RDD[(String, String)] = nameRdd.cartesian(learnRdd)
    value.foreach(println)

    println("===take、top、first===")
    val ints3: Array[Int] = rdd2.take(2)
    ints3.foreach(println)
    val i: Int = rdd2.first()
    println(s"first:${i}")
    val ints4: Array[Int] = rdd2.top(2)
    ints4.foreach(println)
    rdd2.sortBy(x=>x,true).take(2).foreach(println)

    println("===keys,values,mapValues,collectAsMap===")
    val kvRdd = sc.parallelize(Seq(("tom", 90), ("jack", 100), ("mack", 15)), 2)
    println(kvRdd.keys.collect().foreach(println))
    kvRdd.values.collect().foreach(println)
    kvRdd.mapValues(_*2).foreach(println)
    val stringToInt: collection.Map[String, Int] = kvRdd.collectAsMap()
    stringToInt.foreach(println)

    println("===mapPartitionsWithIndex===")
    /*
    mapPartitionsWithIndex[U: ClassTag](
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U]
    */
    val value1: RDD[(String, Int)] = kvRdd.mapPartitionsWithIndex((index, iterable) => {
      println(s"分区号:${index}")
      iterable.map(x => (s"分区号:${index}---Key:${x._1}", x._2 * 3))
    },true)
    value1.foreach(println)

    println("===cache(lazy),persist(lazy),count触发,缓存的等级用的多的是 MEMORY_AND_DISK,unpersist(eager立马执行)===")
  }
}
