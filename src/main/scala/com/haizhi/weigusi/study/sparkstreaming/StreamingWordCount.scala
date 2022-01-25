package com.haizhi.weigusi.study.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName("duan-sparkStreaming")
      val context = new StreamingContext(sparkConf,Seconds(5))
      context
    }

    //接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置。接收器将接收的数据划分为Block以后，按照设置的存储
    //级别对Block进行存储，从TCP Socket中接收数据默认的存储级别为：MEMORY_AND_DISK_SER_2，
    //先存储内存，不足再存储磁盘，存储2副本
    val inputDstream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK)
    val inputDstream1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9988)
    val value: DStream[String] = inputDstream.union(inputDstream1)

    val resDStream: DStream[(String, Int)] = value
      .filter(x => null != x & x.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    resDStream.print(10)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)



  }
}
