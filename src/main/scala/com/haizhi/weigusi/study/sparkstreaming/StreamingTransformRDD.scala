package com.haizhi.weigusi.study.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTransformRDD {
  def main(args: Array[String]): Unit = {
    val ssc = {
      val sparkConf = new SparkConf()
        .setAppName("duan-sparkSreaming")
        .setMaster("local[*]")
      val context = new StreamingContext(sparkConf,Seconds(5))
      context
    }

    val inputDs: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val resDs: DStream[(String, Int)] = inputDs.transform((rdd,time) => {
      val resRdd = {
        rdd.filter(line => null != line && line.trim.length > 0)
          .flatMap(_.split("\\s+"))
          .map((_, 1))
          .reduceByKey(_ + _)
      }
      resRdd
     // resRdd.map(x => (x._1+"@@@"+time,x._2))
    })

    resDs.print(10)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)


  }
}
