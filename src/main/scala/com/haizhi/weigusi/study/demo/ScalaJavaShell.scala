package com.haizhi.weigusi.study.demo

import java.io.{BufferedReader, IOException, InputStreamReader}

import scala.collection.mutable.ListBuffer

object ScalaJavaShell {
  def main(args: Array[String]): Unit = {
    val strings = Array("cat", "/Users/duanbingshun/Desktop/dockerload.txt")
    val str = execShell(strings)
    println(str)
  }

  def execShell(cmds : Array[String]) : String = {
    val process = new java.lang.ProcessBuilder(cmds : _*)
    val resStr = try {
      val p = process.start
      val reader = new BufferedReader(new InputStreamReader(p.getInputStream))
      val listBuff = ListBuffer[String]()
      var line : String = reader.readLine()
      while (line != null) {
        listBuff += line
        line = reader.readLine()
      }
      listBuff.mkString("\n")

    } catch {
      case e: IOException =>
        e.getMessage
    }

    resStr
  }
}
