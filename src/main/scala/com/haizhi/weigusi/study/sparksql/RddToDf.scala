package com.haizhi.weigusi.study.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RddToDf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val dataRdd = sc.textFile("conf/Bosses")

    //一：case class的rdd调用toDf
    println("===一：case class的rdd调用toDf===")
    val resRdd1: RDD[Bosses] = dataRdd.filter(line => null != line && line.trim.split(" ").length == 3)
      .mapPartitions(iter => {
        iter.map(line => {
          //拆箱操作
          val Array(name, age, money) = line.split(" ")
          Bosses(name, age.toInt, money.toLong)
        })
      })
    val resDf1: DataFrame = resRdd1.toDF()
    resDf1.printSchema()
    resDf1.show(10,false)
    //二：Rdd[Row]加上StructType
    println("===二：Rdd[Row]加上StructType===")
    val resRdd2: RDD[Row] = dataRdd.filter(line => null != line && line.trim.split(" ").length == 3)
      .mapPartitions(iter => {
        iter.map(line => {
          val Array(name, age, money) = line.split(" ")
          Row(name, age.toInt, money.toLong)
        })
      })
    val st = StructType(Seq(
      StructField("name",StringType,false),
      StructField("age",IntegerType,false),
      StructField("money",LongType,false)
    ))
    val resDf2 = spark.createDataFrame(resRdd2, st)
    resDf2.printSchema()
    resDf2.show()

    //三：元组的Seq，元组的Rdd调用toDf("name","age")
    println("===三：元组的Seq，元组的Rdd调用toDf(\"name\",\"age\")===")
    val rdd: RDD[(String, Int, Long)] = sc.parallelize(Seq(
      ("zhangsan", 18, 100000000L),
      ("lisi", 12, 1000L),
      ("wangwu", 15, 300000L)
    ))
    val dataFrame = rdd.toDF("name", "age", "money")
    dataFrame.printSchema()
    dataFrame.show()

    val tuples: Seq[(String, Int, Long)] = Seq(
      ("zhangsan", 18, 100000000L),
      ("lisi", 12, 1000L),
      ("wangwu", 15, 300000L)
    )
    val dataFrame1 = tuples.toDF("name", "age", "money")
    dataFrame1.printSchema()
    dataFrame1.show()
  }
}


case class Bosses(
                   name: String,
                   age: Int,
                   money: Long
                 )
