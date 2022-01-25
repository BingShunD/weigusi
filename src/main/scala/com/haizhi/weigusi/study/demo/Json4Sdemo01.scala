package com.haizhi.weigusi.study.demo
import scala.collection.immutable

object Json4Sdemo01 {
  def main(args: Array[String]): Unit = {
    val task = "[\"task_aaa\",\"task_bbb\"]"
    val taskIds: immutable.Seq[String] = JSONUtils.str2Object[List[String]](task)
    taskIds.foreach(println)
  }
}
