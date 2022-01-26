package com.haizhi.weigusi.study.demo

import com.haizhi.weigusi.util.DatasysncConf

import scala.util.Random

object StringUtils {
  def main(args: Array[String]): Unit = {
//    println(getTel("1888317%04d"))
//    println(getTel("188831%04d8"))
//    println(getTel("18883175598"))
//    println(getLength("4878830b4a4f909c3c0973e8680f8f9c"))
//
//    val home = DatasysncConf.flinkxHome
//    println(home)
val strings = Seq("duan", "bing", "shun")
    println(strings.mkString("[\"", "\",\"", "\"]"))


  }

  def getTel(string: String):String = {
    val random = new Random()
    val str = string.format(random.nextInt(10000))
    str
  }

  def getLength(str:String):Int = {
    val length = str.length
    length
  }
}
