package com.haizhi.weigusi.web

import java.util.Date
import javax.servlet.http.HttpServletRequest

import com.haizhi.weigusi.ServerEnv
import com.haizhi.weigusi.api._
import com.haizhi.weigusi.util.{DatasysncBuildInfo, DatasysncConf}

/**
 * Created by jiangmeng on 2021-06-17.
 */
object RestServer {

  // scalastyle:off
  val serverName: String =
    s"""
       |#
       |#              dddddddd
       |#              d::::::d                          tttt
       |#              d::::::d                       ttt:::t
       |#              d::::::d                       t:::::t
       |#              d:::::d                        t:::::t
       |#      ddddddddd:::::d   aaaaaaaaaaaaa  ttttttt:::::ttttttt      aaaaaaaaaaaaa
       |#    dd::::::::::::::d   a::::::::::::a t:::::::::::::::::t      a::::::::::::a
       |#   d::::::::::::::::d   aaaaaaaaa:::::at:::::::::::::::::t      aaaaaaaaa:::::a
       |#  d:::::::ddddd:::::d            a::::atttttt:::::::tttttt               a::::a
       |#  d::::::d    d:::::d     aaaaaaa:::::a      t:::::t              aaaaaaa:::::a
       |#  d:::::d     d:::::d   aa::::::::::::a      t:::::t            aa::::::::::::a
       |#  d:::::d     d:::::d  a::::aaaa::::::a      t:::::t           a::::aaaa::::::a
       |#  d:::::d     d:::::d a::::a    a:::::a      t:::::t    tttttta::::a    a:::::a
       |#  d::::::ddddd::::::dda::::a    a:::::a      t::::::tttt:::::ta::::a    a:::::a
       |#   d:::::::::::::::::da:::::aaaa::::::a      tt::::::::::::::ta:::::aaaa::::::a
       |#    d:::::::::ddd::::d a::::::::::aa:::a       tt:::::::::::tt a::::::::::aa:::a
       |#     ddddddddd   ddddd  aaaaaaaaaa  aaaa         ttttttttttt    aaaaaaaaaa  aaaa
       |#
       |#
       |#      ssssssssssyyyyyyy           yyyyyyy  ssssssssss   nnnn  nnnnnnnn        cccccccccccccccc
       |#    ss::::::::::sy:::::y         y:::::y ss::::::::::s  n:::nn::::::::nn    cc:::::::::::::::c
       |#  ss:::::::::::::sy:::::y       y:::::yss:::::::::::::s n::::::::::::::nn  c:::::::::::::::::c
       |#  s::::::ssss:::::sy:::::y     y:::::y s::::::ssss:::::snn:::::::::::::::nc:::::::cccccc:::::c
       |#   s:::::s  ssssss  y:::::y   y:::::y   s:::::s  ssssss   n:::::nnnn:::::nc::::::c     ccccccc
       |#     s::::::s        y:::::y y:::::y      s::::::s        n::::n    n::::nc:::::c
       |#        s::::::s      y:::::y:::::y          s::::::s     n::::n    n::::nc:::::c
       |#  ssssss   s:::::s     y:::::::::y     ssssss   s:::::s   n::::n    n::::nc::::::c     ccccccc
       |#  s:::::ssss::::::s     y:::::::y      s:::::ssss::::::s  n::::n    n::::nc:::::::cccccc:::::c
       |#  s::::::::::::::s       y:::::y       s::::::::::::::s   n::::n    n::::n c:::::::::::::::::c
       |#   s:::::::::::ss       y:::::y         s:::::::::::ss    n::::n    n::::n  cc:::::::::::::::c
       |#    sssssssssss        y:::::y           sssssssssss      nnnnnn    nnnnnn    cccccccccccccccc
       |#                      y:::::y
       |#                     y:::::y
       |#                    y:::::y
       |#                   y:::::y
       |#                  yyyyyyy
       |#
       |#
      |Version: ${DatasysncBuildInfo.version}
      |Branch: ${DatasysncBuildInfo.branch}
      |Revision: ${DatasysncBuildInfo.revision}
      |Build user: ${DatasysncBuildInfo.buildUser}
      |Build date: ${DatasysncBuildInfo.buildDate}
      |
      |started at: ${new Date().toString}
      |run.mode: ${DatasysncConf.runMode}""".stripMargin

  // scalastyle:on
  private val docsUrl = "/docs"

  private val index =
    <body style="background-color: black;color: greenyellow;">
      <pre>{serverName}</pre>
      <a style="color: greenyellow;" href={docsUrl}>API Docs</a>
    </body>

  private def initApi(): Unit = {
    val indexRender = (request: HttpServletRequest) => index
    JettyUtils.addHandler("/", indexRender)
    // 加载Api路由
    new flinkxApi().mapRoutes()
  }

  def start(): Unit = {
    initApi()
    val host = DatasysncConf.datasysncHost
    val port = DatasysncConf.datasysncPort
    JettyUtils.startJettyServer(host, port, "Datasysnc")
  }

  def stop(): Unit = {
    JettyUtils.stopJettyServer()
  }

  def canStop: Boolean = JettyUtils.requestCount == 0

}

