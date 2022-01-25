package com.haizhi.weigusi.web

import java.lang.management.ManagementFactory

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.xml.Node
import io.prometheus.client.exporter.MetricsServlet
import io.prometheus.client.hotspot.DefaultExports
import io.swagger.v3.jaxrs2.integration.OpenApiServlet
import org.eclipse.jetty.server.{HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, HandlerCollection, HandlerList, JettyStatisticsCollector, ResourceHandler, StatisticsHandler}
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s.JsonDSL._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._
import com.haizhi.weigusi.util.{LazyLogging, UUIDUtils}
import com.haizhi.weigusi.util.ExceptionUtils._

object JettyUtils extends LazyLogging {

  var server: Server = _
  var requestCount = 0

  val contextHandler = new ServletContextHandler()
  contextHandler.setContextPath("/")

  type Responder[T] = HttpServletRequest => T

  class ServletParams[T](val responder: Responder[T],
                                    val contentType: String,
                                    val extractFn: T => String = (in: Any) => in.toString)(implicit ev: T => AnyRef) {}

  implicit def jsonResponderToServlet(responder: Responder[JValue]): ServletParams[JValue] =
    new ServletParams(responder, "application/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): ServletParams[Seq[Node]] =
    new ServletParams(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

  implicit def textResponderToServlet(responder: Responder[String]): ServletParams[String] =
    new ServletParams(responder, "text/plain")


  private def addRequest(): Unit = {
    this.synchronized {
      requestCount += 1
    }
  }

  private def removeRequest(): Unit = {
    this.synchronized {
      requestCount -= 1
    }
  }

  def createServlet[T](servletParams: ServletParams[T])(implicit ev: T => AnyRef): HttpServlet = {
    new HttpServlet {
      def getOrPost[T](servletParams: ServletParams[T],
                                 request: HttpServletRequest,
                                 response: HttpServletResponse)(implicit ev: T => AnyRef): Unit = {
        addRequest()
        val start = System.currentTimeMillis()
        val requestId = UUIDUtils.generate()
        var isSuccess = true
        var statusCode = 0

        try {
          logRequest(request, requestId)
          response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.setHeader("Server", "datasysnc")
          val result = servletParams.responder(request)
          // scalastyle:off println
          response.getWriter.println(servletParams.extractFn(result))
        } catch {
          case NonFatal(e) =>
            isSuccess = false
            statusCode = 1
            logger.error(s"${request.getRequestURI} ${getRequestParam(request)} error: ", e)
            val json = ("status" -> "1") ~ ("errstr" -> e.detailMessage()) ~ ("result" -> "")
            response.getWriter.println(compact(render(json)))
        }

        if (request.getRequestURI != "/") {
          logger.info(s"Request end: $requestId $isSuccess ${System.currentTimeMillis() - start} ms")
        }
        removeRequest()
      }

      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        getOrPost(servletParams, request, response)
      }

      override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        getOrPost(servletParams, request, response)
      }
    }
  }

  private def logRequest(request: HttpServletRequest, requestId: String): Unit = {
    if (request.getRequestURI == "/") {
      return
    }
    var nginxIp = request.getHeader("X-Real-Ip")
    if (nginxIp == null) {nginxIp = request.getRemoteHost}
    logger.info(s"[api] $nginxIp ${request.getMethod} " +
      s"${request.getRequestURI}?${getRequestParam(request)} [$requestId]")
  }

  private def getRequestParam(request: HttpServletRequest) = {
    val sb = new StringBuilder
    import scala.collection.JavaConverters._
    request.getParameterNames.asScala.foreach { key =>
      sb.append(key)
      sb.append('=')
      val value = request.getParameter(key.asInstanceOf[String])
      sb.append(value)
      sb.append("&")
    }
    if (sb.length > 1) {
      sb.deleteCharAt(sb.length - 1)
    }
    sb.toString()
  }

  private def attachPrefix(basePath: String, relativePath: String): String = {
    if (basePath == "") relativePath else (basePath + relativePath).stripSuffix("/")
  }

  def addHandler[T](
    path: String,
    servletParams: ServletParams[T],
    basePath: String = "")(implicit ev: T => AnyRef): Unit = {

    val servlet = createServlet(servletParams)
    val prefixedPath = attachPrefix(basePath, path)
    val holder = new ServletHolder(servlet)
    contextHandler.addServlet(holder, prefixedPath)
  }

  def startJettyServer(
    hostName: String,
    port: Int,
    serverName: String = "",
    maxThreads: Int = 254): Unit = {

    val handlerList = new HandlerList
    val swaggerUIContextHandler = initSwaggerUIEnv()
    val swaggerContextHandler = initSwaggerEnv()
    handlerList.setHandlers(Array(swaggerUIContextHandler, swaggerContextHandler))
    // set form size to 250MB
    contextHandler.setMaxFormContentSize(250 * 1024 * 1024)
    handlerList.addHandler(contextHandler)

    // Bind to the given port, or throw a java.net.BindException if the port is occupied
    val pool = new QueuedThreadPool
    pool.setMaxThreads(maxThreads)
    pool.setDaemon(true)

    server = new Server(pool)
    val httpConnector = new ServerConnector(
      server,
      null,
      // Call this full constructor to set this, which forces daemon threads:
      new ScheduledExecutorScheduler(s"$serverName-JettyScheduler", true),
      null,
      -1,
      -1,
      new HttpConnectionFactory())
    httpConnector.setHost(hostName)
    httpConnector.setPort(port)
    server.setConnectors(Array(httpConnector))
    contextHandler.addServlet(new ServletHolder(new MetricsServlet()), "/prometheus")
    //    val handlers = new HandlerCollection()

    val statisticsHandler = new StatisticsHandler ()
    //    statisticsHandler.setServer(server)
    //    handlers.addHandler(statisticsHandler)

    statisticsHandler.setHandler(handlerList)
    // 注册系统metric
    DefaultExports.initialize()
    // 注册自定义的jetty metric
    new JettyStatisticsCollector(statisticsHandler).register()

    server.setHandler(statisticsHandler)

//    server.setHandler(handlerList)
    try {
      server.start()
      val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
      logger.info(s"$serverName server start listen at: $port, maxThreads $maxThreads, pid is $pid")
      server.join()
    } catch {
      case NonFatal(e) =>
        server.stop()
        pool.stop()
        throw e
    }
  }

  def stopJettyServer(): Unit = {
    server.stop()
  }

  private def initSwaggerEnv(): ServletContextHandler = {

    val servletHolder = new ServletHolder(new OpenApiServlet())
    servletHolder.setInitOrder(1)
    servletHolder.setInitParameter("openApi.configuration.resourcePackages",
      "com.haizhi.datasysnc.api,com.haizhi.datasysnc.util.swagger,com.haizhi.datasysnc.model")
    val swaggerContextHandler = new ServletContextHandler()
    swaggerContextHandler.setContextPath("/api")

    swaggerContextHandler.addServlet(servletHolder, "/*")
    swaggerContextHandler
  }

  private def initSwaggerUIEnv(): ServletContextHandler = {
    val swaggerUIServletHolder = new ServletHolder(new DefaultServlet())
    val resourceBase = JettyUtils.getClass.getClassLoader.getResource("docs").toString
    swaggerUIServletHolder.setInitParameter("resourceBase", resourceBase)

    val swaggerUIContextHandler = new ServletContextHandler()
    swaggerUIContextHandler.setContextPath("/docs")
    swaggerUIContextHandler.addServlet(swaggerUIServletHolder, "/")
    swaggerUIContextHandler
  }
}
