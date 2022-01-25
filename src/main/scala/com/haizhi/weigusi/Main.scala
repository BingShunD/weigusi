package com.haizhi.weigusi

/**
 * Created by jiangmeng on 2021-06-17.
 */
import com.haizhi.weigusi.util.{DatasysncConf, LazyLogging}
import com.haizhi.weigusi.web.RestServer

object Main extends LazyLogging {

  private val configFile = "conf/application.conf"
  System.setProperty("config.file", configFile)

  def main (args: Array[String]): Unit = {
    logger.info(RestServer.serverName)
    logger.info("------------datasysnc config info start-----------------------")
    logger.info(DatasysncConf.getPropeties())
    logger.info("------------datasysnc config info end-----------------------")

    SparkSQLEnv.init()
    listenShutdown()
    RestServer.start()

    def listenShutdown(): Unit = {
      Runtime.getRuntime.addShutdownHook(
        new Thread() {
          override def run(): Unit = {
            while (!RestServer.canStop) {
              Thread.sleep(200)
              logger.info("waiting for rest server exit...")
            }
            SparkSQLEnv.stop()
            Thread.sleep(2000)
            RestServer.stop()
            Thread.sleep(2000)
          }
        }
      )
    }


  }
}


