package com.haizhi.weigusi.service

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, SQLException, Timestamp}
import java.text.SimpleDateFormat

import com.haizhi.weigusi.model.User

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import sys.process._
import com.haizhi.weigusi.{DatasysncException, SparkSQLEnv}
import com.haizhi.weigusi.util.{DatasysncConf, LazyLogging, UUIDUtils}
import org.apache.spark.sql.{DataFrame, Row}


/**
 * Created by jiangmeng on 2021-06-17.
 */

class FlinkxService extends LazyLogging {

  implicit val formats = DefaultFormats
  private val flinkxHome = DatasysncConf.flinkxHome
  private val flinkxMode = DatasysncConf.flinkxMode
  private val flinkxConf = DatasysncConf.flinkxConf
  private val syncplugins = DatasysncConf.syncplugins
  private val flinkxUrl = DatasysncConf.flinkxUrl
  private val channel = DatasysncConf.flinkxChannel
  private val javaHome = System.getenv("JAVA_HOME")

  def startJob(reader : String, writer : String, jobName : String) : JValue = {
    val settings = s"""{
                      |    "speed":{
                      |        "channel":${channel},
                      |        "bytes":0
                      |    },
                      |    "errorLimit":{
                      |        "record":100
                      |    },
                      |    "restore":{
                      |        "maxRowNumForCheckpoint":0,
                      |        "isRestore":false,
                      |        "restoreColumnName":"",
                      |        "restoreColumnIndex":0
                      |    },
                      |    "log":{
                      |        "isLogger":false,
                      |        "level":"warn",
                      |        "path":"",
                      |        "pattern":""
                      |    }
                      |}
                      |""".stripMargin.replaceAll("\\s", " ")

    val jobStr = write(parse(
      s"""
         |{
         |    "job":{
         |        "content":[
         |            {
         |                "reader":${reader},
         |                "writer":${writer}
         |            }
         |        ],
         |        "setting":${settings}
         |    }
         |}
         |""".stripMargin.replaceAll("\\s", " ")))
    logger.info(s"${this.getClass.getSimpleName} jobStr==${jobStr}")

    beforeWriteRecords(writer)

    val cmds = Array(s"${javaHome}/bin/java",
      "-cp", s"${flinkxHome}/lib/*", "com.dtstack.flinkx.launcher.Datasysnc",
      "-mode", flinkxMode,
      "-jobid", jobName,
      "-job", jobStr,
      "-pluginRoot", syncplugins,
      "-flinkconf", flinkxConf
    )

    logger.info(s"${this.getClass.getSimpleName} exec shell [${cmds.mkString(" ")}] result:\n")
    val resStr = execShell(cmds)
    val rg = """Received response \{\"jobUrl\":\"/jobs/.+\"}""".r
    val received = rg.findFirstIn(resStr)
    if(received.isEmpty) {
      throw new DatasysncException(s"lancher flinkx job failed! received shell result: ${resStr}")
    }

    logger.info(s"received: ${received}")
    val json = parse(received.get.substring("Received response ".length))

    val jobId = (json \ "jobUrl").extractOrElse[String]("/jobs/").substring("/jobs/".length)
    val status = if (jobId.nonEmpty) "success" else "failed"
    ("jobId" -> jobId) ~ ("status" -> status)
  }

  def execShell(cmds : Array[String]) : String = {
    val process = new java.lang.ProcessBuilder(cmds : _*)
    val resStr = try {
      val p = process.start
      val reader = new BufferedReader(new InputStreamReader(p.getInputStream))
      val listBuff = ListBuffer[String]()
      var line : String = reader.readLine()
      while (line != null) {
        logger.info(s"[FLINKX] ${line}")
        listBuff += line
        line = reader.readLine()
      }
      listBuff.mkString("\n")

    } catch {
      case e: IOException =>
        e.getMessage
    }

    resStr
  }

  def stopJob(jobId : String) : JValue = {
    val shell = s"curl -XGET ${flinkxUrl}/jobs/${jobId}/yarn-cancel"
    logger.info(s"excuter shell: ${shell}")
    val resStr = shell.!!
    ("jobId" -> jobId) ~ ("status" -> resStr)
  }

  def status(jobId : String) : JValue = {
//    val status = s"curl -XGET ${flinkxUrl}/jobs/${jobId}"
//    logger.info(s"excuter shell: ${status}")
//    val resStr1 = status.!!
//    val json1 = parse(resStr1)
//    val state = (json1 \ "state").extractOrElse[String]("FAILED")
//    val startTime = (json1 \ "start-time").extractOrElse[Long](0)
//    val endTime = (json1 \ "end-time").extractOrElse[Long](0)
//    val duration = (json1 \ "duration").extractOrElse[Int](0)
//
//    val vertices = (json1 \ "vertices").extractOrElse[List[JValue]](Nil)
//
//    val metrics = vertices.map(getVerticeMetric(jobId, _))
//
//    val res = ("state" -> state)~ ("startTime" -> startTime) ~ ("endTime" -> endTime) ~
//      ("duration" -> duration) ~ ("vertices" -> metrics)


     implicit val formats = DefaultFormats
    logger.info("asdasdasd")
   val mapaaa = Map(
      "name" -> "jm",
      "age" -> "17",
    )
    val list = List(mapaaa)

    val res = "aaa";
    val bbb =("jobId" -> jobId) ~ ("status" -> res)
    val aaa = ("jobId" -> jobId) ~ ("status" -> bbb)
    parse(write(list))
   // ("jobId" -> jobId) ~ ("status" -> parse(write(list)))


  }


  def query(sql : String) : JValue = {
    implicit val formats = DefaultFormats
    val spark = SparkSQLEnv.sparkSession
    val df = spark.sql(sql)
    val l = df.count()
    val tuple = formatAsJsonAndReturnLength(df)
    val value = tuple._1
    //val value = "adasd"

    ("jobId" -> sql) ~ ("status" -> value)
  }

  def mysql(sql : String) : JValue = {
    implicit val formats = DefaultFormats
    val name = User.findBy("name", sql).get
    //val value = "adasd"
    ("jobId" -> sql) ~ ("status" -> name.toString)
  }

  /**
   * 将DataFrame转换为json二维数组的形式
   *
   * @param df
   * @return
   */
  protected def formatAsJsonAndReturnLength(df: DataFrame, limitStart: Int = -1, limitEnd: Int = -1,
                                            limitRows: Int = 0): (Seq[ArrayBuffer[String]], Int) = {

    val start = if (limitStart >= 0) limitStart else 0

    var count = 0

    val resArr = if (limitEnd == -1 && start == 0) {
      val limit = if (limitRows == 0) Integer.MAX_VALUE else limitRows
      df.take(limit).map(getRes)
    } else if (limitEnd == -1) {
      count = df.count().toInt
      df.rdd.zipWithIndex().filter(x => x._2 >= start).map(_._1).collect().map(getRes)
    } else {
      count = df.count().toInt
      df.rdd.zipWithIndex().filter(x => x._2 >= start && x._2 <= limitEnd).map(_._1).collect().map(getRes)
    }

    count = if (count > 0) count else resArr.length
    (resArr.toSeq, count)
  }

  val getRes = (row: Row) => {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    row.toSeq.map(d => {
      d match {
        case data: Seq[_] =>
          data.map(elm => {
            elm match {
              case null => "null"
              case i : Double => s"$i"
              case i: Timestamp => s"${sdf.format(i)}"
              case _ => '"' + elm.toString + '"'
            }})
            .mkString("[", ",", "]")
        case _ =>
          if (d != null) {
            d match {
              case i: Double =>
                s"$i"
              case i: Timestamp => sdf.format(i)
              case _ =>
                d.toString
            }
          } else {
            null
          }
      }
    }).to[ArrayBuffer]
  }

  def getVerticeMetric(jobId : String, vertice : JValue) : JValue = {
    val readBytesComplete = (vertice \ "metrics" \ "read-bytes-complete").extractOrElse[Boolean](false)
    val writeBytesComplete = (vertice \ "metrics" \ "write-bytes-complete").extractOrElse[Boolean](false)
    val readRecordsComplete = (vertice \ "metrics" \ "read-records-complete").extractOrElse[Boolean](false)
    val writeRecordsComplete = (vertice \ "metrics" \ "write-records-complete").extractOrElse[Boolean](false)

    val id = (vertice \ "id").extract[String]
    val verticsName = (vertice \ "name").extract[String]
    val keysShell = s"curl -XGET ${flinkxUrl}/jobs/${jobId}/vertices/${id}/metrics"
    logger.info(s"excuter shell: ${keysShell}")
    val resStr = keysShell.!!
    val keysList = parse(resStr).extract[List[JValue]]
      .map(js => (js  \ "id").extract[String])
    val numWriteKeys = keysList.filter(_.endsWith("flinkx.output.numWrite"))
    val byteWriteKeys = keysList.filter(_.endsWith("flinkx.output.byteWrite"))
    val numReadKeys = keysList.filter(_.endsWith("flinkx.output.numRead"))
    val byteReadKeys = keysList.filter(_.endsWith("flinkx.output.byteRead"))
    val getKeys = numWriteKeys ++ byteWriteKeys ++ numReadKeys ++ byteReadKeys

    val metricsShell = s"curl -XGET ${flinkxUrl}/jobs/${jobId}/vertices/${id}/metrics?get=${getKeys.mkString(",")}"
    logger.info(s"excuter shell: ${metricsShell}")
    val metricsStr = metricsShell.!!
    val metricsMap = parse(metricsStr).extractOrElse[List[JValue]](Nil)
      .map(js => ((js \ "id").extract[String], (js \ "value").extract[String]))
      .toMap
    val numWrite = numWriteKeys.map(metricsMap).map(_.toLong).sum
    val byteWrite = byteWriteKeys.map(metricsMap).map(_.toLong).sum
    val numRead = numReadKeys.map(metricsMap).map(_.toLong).sum
    val byteRead = byteReadKeys.map(metricsMap).map(_.toLong).sum

    ("id" -> id) ~ ("name" -> verticsName) ~
      ("read-bytes" -> byteRead) ~ ("read-bytes-complete" -> readBytesComplete) ~
      ("write-bytes" -> byteWrite) ~ ("write-bytes-complete" -> writeBytesComplete) ~
      ("read-records" -> numRead) ~ ("read-records-complete" -> readRecordsComplete) ~
      ("write-records" -> numWrite) ~ ("write-records-complete" -> writeRecordsComplete)
  }


  def beforeWriteRecords(writer : String) : Unit = {
    logger.info(s"excuter beforeWriteRecords ${writer}")
    val jWriter = parse(writer)
    (jWriter \ "name").extract[String] match {
      case "greenplumwriter" => prepareJdbc(jWriter)
      case _ =>
    }
  }

  private def getConn(jdbcUrl : String, user: String, password : String) : Connection = {
    val driverName = jdbcUrl match {
      case _ if (jdbcUrl.startsWith("jdbc:mysql")) => "com.mysql.jdbc.Driver"
      case _ if (jdbcUrl.startsWith("jdbc:oracle")) => "oracle.jdbc.driver.OracleDriver"
      case _ if (jdbcUrl.startsWith("jdbc:postgresql")) => "org.postgresql.Driver"
      case _ if (jdbcUrl.startsWith("jdbc:pivotal:greenplum")) => "com.pivotal.jdbc.GreenplumDriver"
      case _ => throw new DatasysncException(s"not support jdbcUrl=${jdbcUrl}")
    }
    // scalastyle:off classforname
    Class.forName(driverName)
    DriverManager.getConnection(jdbcUrl, user, password)
  }

  private def quoteIdentifier(jdbcUrl : String, column : String) : String = {
    jdbcUrl match {
      case _ if (jdbcUrl.startsWith("jdbc:mysql")) => s"""`${trimIdentifier(column, "`")}`"""
      case _ if (jdbcUrl.startsWith("jdbc:oracle")) => s""""${trimIdentifier(column, "\"")}""""
      case _ if (jdbcUrl.startsWith("jdbc:postgresql")) => s""""${trimIdentifier(column, "\"")}""""
      case _ if (jdbcUrl.startsWith("jdbc:pivotal:greenplum")) => s""""${trimIdentifier(column, "\"")}""""
      case _ => throw new DatasysncException(s"not support jdbcUrl=${jdbcUrl}")
    }
  }

  private def trimIdentifier(column: String, identifier: String) : String = {
    if (column.startsWith(identifier) && column.endsWith(identifier)) {
      val length = column.length
      val identifierLength = identifier.length
      column.substring(identifierLength, length - identifierLength)
    } else {
      column
    }
  }

  def getReaderColumn(reader : JValue) : List[String] = {
    (reader \ "hdfsreader").extract[String] match {
      case "hdfsreader" => (reader \ "parameter" \ "column")
        .extract[Array[JValue]]
        .zipWithIndex
        .map(t => ((t._1 \ "index").extractOrElse[Int](t._2), (t._1 \ "name").extract[String]))
        .sortBy(_._1)
        .map(_._2)
        .toList

      case _ => (reader \ "parameter" \ "column")
        .extract[Array[JValue]]
        .zipWithIndex
        .map(t => ((t._1 \ "index").extractOrElse[Int](t._2), (t._1 \ "name").extract[String]))
        .sortBy(_._1)
        .map(_._2)
        .toList
    }
  }

  def prepareJdbc(writer : JValue) : Unit = {
    val parameter = writer \ "parameter"
    val connectionList = (parameter \ "connection").extract[List[JValue]]
    val user = (parameter \ "username").extract[String]
    val password = (parameter \ "password").extract[String]

    val destColumn = (parameter \ "column").extract[Array[JValue]]
      .map(json => (
        (json \ "name").extract[String],
        if ((json \ "type").extract[String]=="double") {
          "double precision"
        } else if ((json \ "type").extract[String]=="string") {
          "text"
        } else {
          (json \ "type").extract[String]
        }
      ))
      .toList

    connectionList.foreach( connection => {
      val jdbcUrl = (connection \ "jdbcUrl").extract[String]
      val conn = getConn(jdbcUrl, user, password)
      val tables = (connection \ "table").extract[List[String]]
      for(table <- tables) {
        if (!tableExists(jdbcUrl, conn, table)) {
          val sql = getCreateSql(jdbcUrl, destColumn, table)
          val statement = conn.createStatement()
          try {
            logger.info(s"execute sql = ${sql}")
            statement.execute(sql)
          } finally {
            statement.close()
          }
        }
        val tableColumns: Seq[String] = getTableSchema(conn, table)
        val addCol = destColumn.filter(dc => !tableColumns.contains(dc._1))
        addColumns(conn, addCol, table, jdbcUrl)
      }
      conn.close()
    })
  }

  def getCreateSql(jdbcUrl : String, columns : List[(String, String)], table : String) : String = {
    val columnStr = columns.map(col => s"${quoteIdentifier(jdbcUrl, col._1)} ${col._2}")
      .mkString(",")
    s"create table ${quoteIdentifier(jdbcUrl, table)} (${columnStr})"
  }

  def tableExists(jdbcUrl: String, conn: Connection, table : String): Boolean = {
    Try {
      val statement = conn.prepareStatement(s"SELECT * FROM ${quoteIdentifier(jdbcUrl, table)} WHERE 1=0")
      try {
        statement.setQueryTimeout(10)
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  def addColumns(conn: Connection, columns: List[(String, String)], table : String, jdbcUrl : String): Unit = {
    try {
      val statement = conn.createStatement()
      try {
        columns.foreach(col => {
          val alterSql =
            s"""
               |alter table ${quoteIdentifier(jdbcUrl, table)}
               |add column ${quoteIdentifier(jdbcUrl, col._1)}  ${col._2} null
               |""".stripMargin.replaceAll("\\s", " ")
          logger.info(s"${this.getClass.getSimpleName} excuter sql = ${alterSql}")
          statement.execute(alterSql)
        })
      } catch {
        case _: SQLException =>
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException =>
    }
  }

  def getTableSchema(conn: Connection, table : String): List[String] = {
    try {
      val statement = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0")
      try {
        statement.setQueryTimeout(10)
        getSchema(statement.executeQuery())
      } catch {
        case _: SQLException => Nil
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => Nil
    }
  }

  def getSchema(resultSet: ResultSet): List[String] = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    (1 to ncols).map(rsmd.getColumnLabel).toList
  }

}
