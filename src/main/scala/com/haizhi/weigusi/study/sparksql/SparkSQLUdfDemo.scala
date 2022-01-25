package com.haizhi.weigusi.study.sparksql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLUdfDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    import spark.implicits._

    val datas: DataFrame = spark.read.json("conf/worker.json")
//    datas.printSchema()
//    datas.show()

    println("===Sql===")
    spark.udf.register("low2up",
      (name:String) => {name.toUpperCase}
    )
    datas.createOrReplaceTempView("myTb")
    spark.sql("select *,low2up(name) as upName from myTb").show()

    println("===DSL===")
    import org.apache.spark.sql.functions._
    val myfunc: UserDefinedFunction = udf(
      (name: String) => {
        name.toUpperCase
      }
    )
    datas.select(
      $"money",$"name",myfunc($"name").as("upName")
    ).show()
  }
}
