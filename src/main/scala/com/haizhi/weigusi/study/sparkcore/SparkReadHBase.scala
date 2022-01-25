package com.haizhi.weigusi.study.sparkcore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 从HBase 表中读取数据，封装到RDD数据集
 */
object SparkReadHBase {
  def main(args: Array[String]): Unit = {
    // 创建应用程序入口SparkContext实例对象
    val sc: SparkContext = {
      // 1.a 创建SparkConf对象，设置应用的配置信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
        // TODO: 设置使用Kryo 序列化方式
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // TODO: 注册序列化的数据类型
        .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
      // 1.b 传递SparkConf对象，构建Context实例
      new SparkContext(sparkConf)
    }
    sc.setLogLevel("WARN")
    // TODO: a. 读取HBase Client 配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    // TODO: b. 设置读取的表的名称
    conf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")
    /*
    def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration = hadoopConfiguration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]
    ): RDD[(K, V)]
    */
    val resultRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf, //
      classOf[TableInputFormat], //
      classOf[ImmutableBytesWritable], //
      classOf[Result] //
    )
    println(s"Count = ${resultRDD.count()}")
    resultRDD
      .take(5)
      .foreach { case (rowKey, result) =>
        println(s"RowKey = ${Bytes.toString(rowKey.get())}")
        // HBase表中的每条数据封装在result对象中，解析获取每列的值
        result.rawCells().foreach { cell =>
          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
          val column = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          val version = cell.getTimestamp
          println(s"\t $cf:$column = $value, version = $version") } }
    // 应用程序运行结束，关闭资源
    sc.stop()
  } }