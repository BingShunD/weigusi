package com.haizhi.weigusi.util

import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging {
  PropertyConfigurator.configure("conf/log4j.properties")
  protected lazy val logger: Logger = LoggerFactory.getLogger("datasysnc")
}
