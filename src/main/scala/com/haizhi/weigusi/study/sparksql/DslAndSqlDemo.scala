package com.haizhi.weigusi.study.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DslAndSqlDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val datas: Dataset[String] = spark.read.textFile("conf/ratings.dat")
    val datasDf = datas.filter(line => null != line && line.trim.split("::").length == 4)
      .mapPartitions(iter => {
        iter.map(line => {
          val Array(userId, movieId, rating, timestamp) = line.split("::")
          (userId, movieId, rating.toDouble, timestamp.toLong)
        })
      })
      .toDF("userId", "movieId", "rating", "timestamp")

    //println(datasDf.queryExecution)
    println("===Sql===")
    datasDf.createOrReplaceTempView("myTb")
    //需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
    /*
      +------+-------+------+---------+
      |userId|movieId|rating|timestamp|
      +------+-------+------+---------+
      |1     |1193   |5.0   |978300760|
      |1     |661    |3.0   |978302109|
      |1     |914    |3.0   |978301968|
      |1     |3408   |4.0   |978300275|
      |1     |2355   |5.0   |978824291|
      |1     |1197   |3.0   |978302268|
      |1     |1287   |5.0   |978302039|
      |1     |2804   |5.0   |978300719|
      |2     |2628   |3.0   |978300051|
      |2     |1103   |3.0   |978298905|
      +------+-------+------+---------+
      */
    val resDf = spark.sql(
      """
        |select movieId,round(avg(rating),2) as avg_rating,count(1) as cnt
        |from myTb
        |group by movieId
        |having cnt > 0
        |order by avg_rating desc,cnt desc
        |limit 10
        |""".stripMargin)

    println(resDf.queryExecution)
    resDf.printSchema()
    resDf.show()

    println("===DSL===")
    //基于DSL=Domain Special Language（特定领域语言） 分析
    import org.apache.spark.sql.functions._
    val frame = datasDf.select($"movieId", $"rating")
        .groupBy($"movieId")
        .agg(
          round(avg($"rating"),2).as("avg_rating"),
          count($"movieId").as("cnt")
        )
        .filter($"cnt">0)
        .orderBy($"avg_rating" desc,$"cnt" desc)
        .limit(10)

    frame.printSchema()
    frame.show()


    val value: RDD[String] = datasDf.select("movieId").rdd.map(_.getString(0))
    value.foreach(println)
   // value.foreachPartition()
    val str: String = value.max()
    println(s"max:${str}")


    Thread.sleep(10000000)
    spark.stop()

  }
}
