package com.haizhi.weigusi.util

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.io.File


/**
 * Created by jiangmeng on 2021-06-17.
 */
object DatasysncConf {

  private val configFile = {
    if (File(s"conf/${System.getProperty("user.name")}.conf").exists) {
      s"conf/${System.getProperty("user.name")}.conf"
    } else {
      "conf/application.conf"
    }
  }

  private val specialConfig: Config = ConfigFactory.parseFile(new java.io.File(configFile))

  private val commonfile = new java.io.File("conf/common.conf")

  private val conf = if (commonfile.exists()) {
    ConfigFactory.load(specialConfig).withFallback(ConfigFactory.parseFile(commonfile))
  } else {
    ConfigFactory.load(specialConfig)
  }

    private def getConf[T](name: String, default: T): T = {
    try {
      conf.getValue(name).unwrapped().asInstanceOf[T]
    } catch {
      case _: Missing => default
    }
  }

  private def getConf[T](name: String): T = {
    conf.getValue(name).unwrapped().asInstanceOf[T]
  }

  def getPropeties(): String = {
    import scala.collection.JavaConverters._
    conf.entrySet().asScala.map(proper => {
      s"key: ${proper.getKey}, value: ${proper.getValue}\n"
    }).mkString(",")
  }

  val taskTimeout = getConf[Int]("viewOptimize.timeout", 120) // 任务超时, 单位:分钟

  // 线上环境一直用的是 dev，本地开发自测的时候建议使用 dev_test
  lazy val runMode: String = System.getProperty("run.mode", "dev")

  val datasysncHost = getConf[String]("server.host", "0.0.0.0")

  val datasysncPort = getConf[Int]("server.port", 3000)

  val flinkxHome = getConf[String]("flinkx.home", "flinkx")

  val flinkxMode = getConf[String]("flinkx.mode", "standalone")

  val flinkxConf = getConf[String]("flinkx.conf", "flinkx/conf")

  val syncplugins = getConf[String]("flinkx.syncplugins", "flinkx/syncplugins")

  val flinkxUrl = getConf[String]("flinkx.url", "http://localhost:8081")

  val flinkxChannel = getConf[Int]("speed.channel", 1)

  val redisHost = getConf[String]("redis.host","localhost")

  val redisPort = getConf[Int]("redis.port")

  val redisPassword = getConf[String]("redis.password")
}


