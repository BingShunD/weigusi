package com.haizhi.weigusi.study.demo

import org.json4s.{DefaultFormats, FieldSerializer, Formats, JValue}
import org.json4s.Extraction.extract
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

/**
 * 使用 json4s 序列化
 * 详细参看官方文档 Serialization
 * https://github.com/json4s/json4s#serialization
 */
object JSONUtils {
  implicit val formats: Formats = DefaultFormats

  /**
   * 同时支持 case class 构造参数 + 1个 类 字段 序列化方法 [如果想要多个 那就 多个 FieldSerializer]
   * @param caseObj 需要序列化字段的 caseObj
   * @tparam Assist 字段类型
   * @return JSONString
   */
  def casePlus2JSONStr[Assist: Manifest](caseObj: AnyRef): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[Assist]()
    Serialization.write(caseObj)
  }

  def jsonStr2casePlus[Master: Manifest, Assist: Manifest](noCaseJSONStr: String): Master = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[Assist]()
    Serialization.read[Master](noCaseJSONStr)
  }

  /**
   * 仅支持 case class 构造参数
   * @param caseObj 样例类对象
   * @return JSONString
   */
  def case2JSONStr(caseObj: AnyRef): String = {
    Serialization.write(caseObj)
  }

  def jsonStr2Case[T: Manifest](jsonStr: String): T = {
    Serialization.read(jsonStr)
  }

  /**
   * 注: 该方法及 jValueExtract2Case 仅为了 少写点 import & DefaultFormats
   * @param str 被解析字符串
   * @return JValue对象
   */
  def parseStr2JValue(str: String): JValue = {
    parse(str)
  }

  def jValueExtract2Case[T: Manifest](jValue: JValue): T = {
    extract(jValue)
  }

  def str2Object[T: Manifest](str: String): T = {
    parse(str).extract[T]
  }
}
