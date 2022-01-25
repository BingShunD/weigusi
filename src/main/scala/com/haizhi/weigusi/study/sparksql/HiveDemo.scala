package com.haizhi.weigusi.study.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveDemo {
  def main(args: Array[String]): Unit = {
    /*    val f1: File = new File("/Users/duanbingshun/IdeaProjects/fordatax_1/data")
        val f2: File = new File("/Users/duanbingshun/IdeaProjects/fordatax_1/data","duanbs.txt")
        //f2.createNewFile()
        val path: String = f2.getPath
        val str = path.replace("duanbs", "duanbingshun")
        val bool = new File(str).createNewFile()
    //    val f2est = f2.exists().toString
    //    println(s"path=$str,f2est=$f2est")*/

    /*    def rename(from: String, to: String): Unit = {
          val fs = FileSystem.get(new Configuration)
          val hdfsPath = new Path(from)
          val targetPath = new Path(to).getParent
          if (!fs.exists(targetPath)) {
            fs.mkdirs(targetPath)
          }
          if (fs.exists(hdfsPath)) {
            fs.rename(hdfsPath, new Path(to))
          }
        }

        rename("/Users/duanbingshun/IdeaProjects/fordatax_1/data01/flink.txt","/Users/duanbingshun/IdeaProjects/fordatax_1/data")*/

    // val fs = FileSystem.get(new Configuration)

    //  fs.rename(new Path("/Users/duanbingshun/IdeaProjects/fordatax_1/data/flink.txt"),new Path("/Users/duanbingshun/IdeaProjects/fordatax_1/data01"))
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("hive_to_hive")
      // 设置输出文件算法
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")

    val builder: SparkSession.Builder = SparkSession.builder()
      .config(sparkConf)

    builder
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")

    val spark: SparkSession = builder.getOrCreate()

    val inputDf = spark.sql("select * from duanbs.employee")
    //    inputDf.printSchema()
    //    inputDf.show(10,false)
    //    println(inputDf.rdd.collect().toSeq.mkString(","))
    inputDf.createOrReplaceTempView("inputTb")

    //val frame1 = spark.sql("select eud,name,salary,destination from inputTb")
    val frame2 = spark.sql("select eud,name,salary,destination from inputTb where salary > 40000")

    frame2.show(100, false)
    frame2.printSchema()

    println("~~~~~~~~~~~~~~~~~~~~~~~~~")
    val qe2 = frame2.queryExecution
    println(qe2.executedPlan)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~")
    println(qe2.debug.codegen)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~")
    println(qe2.toRdd.toDebugString)

    Thread.sleep(1000000)
    spark.stop()
    // val qe1 = frame1.queryExecution
    //val qe2 = frame2.queryExecution

    //l.treeString  l.children(0).prettyJson  l.children.length
    // val logical = qe1.logical

    //    val value: Dataset[String] = spark.read.textFile("hdfs://localhost:8020/input/aaa.txt")
    //    value.printSchema()
    //    value.show(10,false)
    //
    //    value.write.parquet("hdfs://localhost:8020/parquetOut")

  }
}
