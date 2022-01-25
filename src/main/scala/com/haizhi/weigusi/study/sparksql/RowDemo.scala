package com.haizhi.weigusi.study.sparksql

import org.apache.spark.sql._

object RowDemo {
  def main(args: Array[String]): Unit = {
    val row: Row = Row("zhangsan", "man", 18)
    println(row)
    //fieldIndex on a Row without schema is undefined
    //val value: Any = row.getAs[String]("value")
    //println(value)

    println("===fromSeq===")
    val row1: Row = Row.fromSeq(Seq(
      ("lisi", "man", 20),("wangwu", "man", 21),("zhaoliu", "man", 22)
    ))
    println(row1)
  }
}
