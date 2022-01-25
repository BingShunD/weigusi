package com.haizhi.weigusi.util

import java.security.MessageDigest

/**
 * Created by jiangmeng on 2021-06-17.
 */
object UUIDUtils {

  def generate(): String = {
    val uuid = java.util.UUID.randomUUID()
    val result = uuid.toString.toLowerCase.replace("-", "")
    result
  }

  def md5(str: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val md5Data = md5.digest(str.getBytes)
    val sb = new StringBuffer
    md5Data.foreach { d =>
      val v = d.toInt & 0xff
      if (v < 16) {
        sb.append('0')
      }
      sb.append(Integer.toHexString(v))
    }
    sb.toString
  }
}
