package com.haizhi.weigusi.study.sparkcore

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object InputFormatOutputFormatDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("duan-spark").getOrCreate()
    val sc = spark.sparkContext
    /*
        newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
          path: String,
          fClass: Class[F],
          kClass: Class[K],
          vClass: Class[V],
          conf: Configuration = hadoopConfiguration): RDD[(K, V)]
        */
    val rdd: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(
      "conf/wordtest",
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    )

    val value = rdd.map(_._2.toString)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    /*
        saveAsNewAPIHadoopFile(
          path: String,
          keyClass: Class[_],
          valueClass: Class[_],
          outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
          conf: Configuration = self.context.hadoopConfiguration): Unit
        */
    value.saveAsNewAPIHadoopFile(
      s"conf/output-${System.nanoTime()}",
      classOf[Text],
      classOf[IntWritable],
      classOf[TextOutputFormat[Text, IntWritable]]
    )

    Thread.sleep(1000000)
    sc.stop()


  }
}
