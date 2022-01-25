package com.haizhi.weigusi.web

import com.haizhi.weigusi.util.LazyLogging
import com.haizhi.weigusi.web.JettyUtils._
import org.json4s._

/**
 * Created by jiangmeng on 2021-06-17.
 */
trait ApiComponent extends LazyLogging {
  val routes: Map[String, Responder[JValue]]
  val basePath: String

  def mapRoutes(): Unit = {
    routes.foreach { (info: (String, Responder[JValue])) =>
      JettyUtils.addHandler(info._1, info._2, basePath)
    }
  }

}
