package com.haizhi.weigusi.study.sparksql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSqlStart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    import spark.implicits._

    val value: Dataset[String] = spark.read.textFile("conf/wordtest")
    val value1 = value.filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))

    println("===DSL===")
    val df = value1.groupBy("value")
      .count()
    df.show()
    df.printSchema()

    println("===Sql===")
    value1.createOrReplaceTempView("mytb")
    val dataFrame = spark.sql(
      """
        |select value,count(1) as cnt from mytb group by value order by cnt desc
        |""".stripMargin)
    dataFrame.show()
    dataFrame.printSchema()







//        Thread.sleep(1000000)
//        spark.stop()
  }
}
