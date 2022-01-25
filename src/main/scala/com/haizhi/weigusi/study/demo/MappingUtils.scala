package com.haizhi.weigusi.study.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

object MappingUtils {
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

  def getCatalogTable(dbtable: String): CatalogTable = {
    val table = spark.sessionState.sqlParser.parseTableIdentifier(dbtable)
    spark.sessionState.catalog.getTableMetadata(table)
  }

  def getHiveTableFromView(tableName: String): Seq[String] = {
    val viewText = getCatalogTable(tableName).viewText
    val value = viewText.get
    println("===@@@" + value + "@@@===")
    val analyzed: LogicalPlan = spark.sql(viewText.get).queryExecution.analyzed
    println("===@@@" + analyzed + "@@@===")
    //collect[B](pf: PartialFunction[BaseType, B]): Seq[B]
    val strings = analyzed.collect {
      case HiveTableRelation(tableMeta, _, _) => tableMeta.identifier.toString()
      case LogicalRelation(_, _, Some(tableMeta), _) => tableMeta.identifier.toString()
    }
    strings.foreach(println)
    if (viewText.isDefined) {
      spark.sql(viewText.get)
        .queryExecution.analyzed
        .collect {
          case HiveTableRelation(tableMeta, _, _) => tableMeta.identifier.toString()
          case LogicalRelation(_, _, Some(tableMeta), _) => tableMeta.identifier.toString()
        }.distinct
    } else {
      Nil
    }
  }

  def main(args: Array[String]): Unit = {
    val table = getHiveTableFromView("duanbs.union_view")
    table.foreach(println)
  }
}
