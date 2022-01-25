package com.haizhi.weigusi.study.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object StructTypeDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    import spark.implicits._

    val frame: DataFrame = spark.read.json("conf/worker.json")
    frame.printSchema()
    frame.show()
    val row: Row = frame.first()
    //val value: Any = row(0)
    println(row(0))
    println(row(1))
    println(row.getDouble(0))
    println(row.getString(1))
    println(row.getAs[Double](0))
    println(row.getAs[String](1))
    println(row.getAs[String]("name"))

    println("~~~~~~~~~~~~~")
    val rows: Array[Row] = frame.take(1)
    rows.foreach(println)
    val schema: StructType = frame.schema
    println(schema)
  }
}
