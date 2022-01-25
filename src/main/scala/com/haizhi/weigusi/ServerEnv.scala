package com.haizhi.weigusi

import java.util.concurrent.{ConcurrentHashMap, Future, TimeUnit}

import com.haizhi.weigusi.service._
import com.haizhi.weigusi.util.{DatasysncConf, LazyLogging}

/**
 * Created by jiangmeng on 2021-06-17.
 */
object ServerEnv extends LazyLogging {
  val flinkxService = new FlinkxService()
}
