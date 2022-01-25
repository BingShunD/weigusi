package com.haizhi.weigusi.study.structuredstreaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

class MySQLForeachWriter extends ForeachWriter[Row]{
  var conn:Connection = _
  var pstmt:PreparedStatement = _
  val insertSQL = "replace into `tb_word_count` (id,word,count) values (NULL,?,?)"

  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn = DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/duanbs?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "root",
      "root"
    )
    pstmt = conn.prepareStatement(insertSQL)
    true
  }

  override def process(value: Row): Unit = {
    pstmt.setString(1,value.getAs[String]("value"))
    pstmt.setLong(2,value.getAs[Long]("count"))
    pstmt.executeUpdate()
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (null != pstmt) pstmt.close()
    if(null != conn) conn.close()
  }
}
