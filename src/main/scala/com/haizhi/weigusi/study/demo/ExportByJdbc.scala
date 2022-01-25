package com.haizhi.weigusi.study.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

object ExportByJdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("export")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val inputDf = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "duanbs.tb_word_count")
      .load()

    inputDf.show(10, false)
    inputDf.printSchema()

    val dfFields = inputDf.schema.fields.map(_.name)

    val schema: Array[StructField] = inputDf.schema.fields
    schema.foreach(fields => {
      println(fields.name)
      println(fields.dataType.typeName)
    })

    val addFields = Seq(
      s"cast(null as string) as `school`",
      s"cast(null as integer) as `grand`",
      s"cast(word as integer) as `game`",
      s"cast(word as string) as `lol`",
      s"cast(count as integer) as `chiji`",
    )

    val selectArr = dfFields.map(name => s"`${name}`") ++ addFields
    selectArr.foreach(println)

    println("===========")
    val newDf = inputDf.selectExpr(selectArr: _*)
    newDf.show()
    newDf.printSchema()

  }
}
