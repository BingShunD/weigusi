package com.haizhi.weigusi.study.esdemo

import com.haizhi.weigusi.util.ESClientConnectionUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object EsDemo {
  def main(args: Array[String]): Unit = {


    readEs


  }


  def readEs = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("duanbs")
      .config("pushdown", "true")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .getOrCreate()

    val query1 = """{
                  |  "query": {
                  |    "match_all": {}
                  |  }
                  |}""".stripMargin

    val bool = """{
                   |  "query": {
                   |    "bool": {
                   |      "must": [
                   |        {"range": {
                   |          "id": {
                   |            "gte": 6,
                   |            "lte": 10
                   |          }
                   |        }}
                   |      ]
                   |    }
                   |  }
                   |}""".stripMargin

    val value: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(spark.sparkContext, "duanbs", bool)

    value.foreach(println)

    println("==========")

        value.take(100).foreach(row => {
          //读取出来的数据格式(ID,Map(String,AnyRef))类型
          val map: collection.Map[String, AnyRef] = row._2
          //输出Map中的数据
          for(s <- map)
            println(s._1 + "  " + s._2)
          println()
        })
  }

  def insertIntoEs = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("duanbs")
      .getOrCreate()
    import spark.implicits._
    val inputDf = Seq(
      ("希拉里不拉稀",6,"希拉里"), ("奥巴马很白",7,"马巴奥"), ("鸣人色诱术",8,"鸣人"), ("鸣人丸子最大",9,"鸣人"), ("鸣人爱佐助",10,"鸣人")
    ).toDF("subject","id","name")
    inputDf.printSchema()
    inputDf.show(10,false)
    val mapOptionBase = Map(
      //"es.nodes.wan.only" -> "false",
      "es.nodes" -> "localhost",
      "es.port" -> "9200",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "es.batch.write.retry.count" -> "6",
      "es.batch.write.retry.wait" -> "120s",
      "index.refresh_interval" -> "-1",
      "index.translog.durability" -> "async",
      "es.batch.size.bytes" -> "10m",
      "es.batch.size.entries" -> "10000"
    )
    val mapMappingId = "es.mapping.id" -> "id"
    val upsertOption = mapOptionBase + ("es.write.operation" -> "index") + mapMappingId
    val ds: Dataset[String] = inputDf.toJSON
    println(ds.toString())
    ds.rdd.saveJsonToEs("duanbs",upsertOption)
  }

  def createIndex = {
    val mapping = """{
                    |  "settings": {
                    |    "analysis": {
                    |      "analyzer": {
                    |        "ik":{
                    |          "tokenizer":"ik_max_word"
                    |        }
                    |      }
                    |    }
                    |  },
                    |  "mappings": {
                    |    "dynamic":true,
                    |    "properties": {
                    |      "subject":{
                    |        "type": "text",
                    |        "analyzer": "ik_max_word"
                    |      }
                    |    }
                    |  }
                    |}""".stripMargin
    ESClientConnectionUtil.createIndex("duanbs","_doc",mapping,"localhost","9200")
    println("创建index:")
  }

  def deleteIndex = {
    ESClientConnectionUtil.deleteIndex("duanbs","localhost","9200")
    println("删除index:")
  }
}
