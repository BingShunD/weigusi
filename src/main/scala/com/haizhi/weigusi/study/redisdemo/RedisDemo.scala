package com.haizhi.weigusi.study.redisdemo

import com.haizhi.weigusi.service.RedisService

object RedisDemo {
  def main(args: Array[String]): Unit = {
    val maybeString = RedisService.getValue("name")
    println(maybeString.get)
  }
}
