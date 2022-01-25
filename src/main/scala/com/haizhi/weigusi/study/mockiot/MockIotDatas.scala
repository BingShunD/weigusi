package com.haizhi.weigusi.study.mockiot

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats

import scala.util.Random

object MockIotDatas {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    val deviceTypes = Array(
      "db", "bigdata", "kafka", "route", "bigdata", "db", "bigdata", "bigdata", "bigdata"
    )
    val random = new Random()

    implicit val formats = DefaultFormats
    import org.json4s.jackson.Serialization._
    while (true) {
      val index = random.nextInt(deviceTypes.length)
      val deviceId: String = s"device_${(index +1) * 10 + random.nextInt(index + 1)}"
      val deviceType: String = deviceTypes(index)
      val deviceSignal: Int = 10 + random.nextInt(90)
      // 模拟构造设备数据
      val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())
      val deviceJson = write(deviceData)
      println(deviceJson)
      Thread.sleep(100 + random.nextInt(500))
      //ProducerRecord(String topic, K key, V value)
      val record = new ProducerRecord[String, String]("iotTopic", deviceId, deviceJson)
      producer.send(record)
    }
    producer.close()
  }
}
