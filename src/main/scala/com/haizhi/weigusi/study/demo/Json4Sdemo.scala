package com.haizhi.weigusi.study.demo

import com.haizhi.weigusi.study.mockiot.DeviceData
import org.json4s.jackson.Json
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization._

import scala.util.Random

object Json4Sdemo {
  def main(args: Array[String]): Unit = {
    val deviceTypes = Array(
      "db", "bigdata", "kafka", "route", "bigdata", "db", "bigdata", "bigdata", "bigdata"
    )
    val random = new Random()
    val index = random.nextInt(deviceTypes.length)
    val deviceId: String = s"device_${(index +1) * 10 + random.nextInt(index + 1)}"
    val deviceType: String = deviceTypes(index)
    val deviceSignal: Int = 10 + random.nextInt(90)
    // 模拟构造设备数据
    val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())

    implicit val formats = DefaultFormats
    val str = write(deviceData)
    println(str)

    //val deviceJson: String = new Json(org.json4s.DefaultFormats).write(deviceData)

    val value: JValue = parse(""" { "numbers" : [1, 2, 3, 4] } """)
    val value1: JValue = parse("""{"name":"Toy","price":35.35}""", useBigDecimalForDouble = true)
    val value2: JValue = value \ "numbers"
    val value3: JValue = value1 \ "price"
    println(value)
    println(value1.toString)
    println(value2)
    println(value3)

  }
}
