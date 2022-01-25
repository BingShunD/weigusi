package com.haizhi.weigusi.api

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.{Consumes, Path, POST, Produces}

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema}
import io.swagger.v3.oas.annotations.tags.Tag
import org.json4s.{JValue, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import com.haizhi.weigusi.ServerEnv
import com.haizhi.weigusi.service.{FlinkxService}
import com.haizhi.weigusi.util.{DatasysncConf, LazyLogging}
import com.haizhi.weigusi.util.RequestUtils._
import com.haizhi.weigusi.web.ApiComponent
import com.haizhi.weigusi.web.JettyUtils._


/**
 * Created by jiangmeng on 2021-06-17.
 */

@Path(value = "/flinkx")
@Produces(value = Array("appliction/json"))
@Tag(name = "JOB", description = "Mobius执行job任务")
class flinkxApi extends ApiComponent with LazyLogging {

  private val flinkxService = new FlinkxService()

  implicit val formats = DefaultFormats

  override val basePath = "/flinkx"
  override val routes = Map[String, Responder[JValue]] (
    "/start" -> start,
    "/stop" -> stop,
    "/status" -> status,
    "/query" -> query
  )

  @POST
  @Path(value = "/stop")
  @Operation(
    summary = "触发当前表导出任务",
    description = "触发当前导出任务",
    parameters = Array(
      new Parameter(
        name = "jobId",
        in = ParameterIn.QUERY,
        description = "jobId",
        required = true,
        array = new ArraySchema(
          schema = new Schema(implementation = classOf[String])
        )
      )
    )
  )
  @Produces(value = Array("application/json"))
  @Consumes(value = Array("application/json"))
  def stop: Responder[JValue] = (request: HttpServletRequest) => {
    val jobId = request.get("jobId")
    val res = flinkxService.stopJob(jobId)
    ("status" -> "0") ~ ("errstr" -> "") ~ ("result" -> res)
  }

  @POST
  @Path(value = "/status")
  @Operation(
    summary = "触发当前表导出任务",
    description = "触发当前导出任务",
    parameters = Array(
      new Parameter(
        name = "shell",
        in = ParameterIn.QUERY,
        description = "shell",
        required = true,
        array = new ArraySchema(
          schema = new Schema(implementation = classOf[String])
        )
      )
    )
  )
  @Produces(value = Array("application/json"))
  @Consumes(value = Array("application/json"))
  def status: Responder[JValue] = (request: HttpServletRequest) => {
    val jobId = request.get("jobId")
    val res = flinkxService.status(jobId)
    ("status" -> "0") ~ ("errstr" -> "") ~ ("result" -> res)
  }

  @POST
  @Path(value = "/start")
  @Operation(
    summary = "触发当前表导出任务",
    description = "触发当前导出任务",
    parameters = Array(
      new Parameter(
        name = "reader",
        in = ParameterIn.QUERY,
        description = "flinkx的reader参数",
        required = true,
        array = new ArraySchema(
          schema = new Schema(implementation = classOf[String])
        )
      ),
      new Parameter(
        name = "writer",
        in = ParameterIn.QUERY,
        description = "flinkx的writer参数",
        required = true,
        array = new ArraySchema(
          schema = new Schema(implementation = classOf[String])
        )
      )
    )
  )
  @Produces(value = Array("application/json"))
  @Consumes(value = Array("application/json"))
  def start: Responder[JValue] = (request: HttpServletRequest) => {
    val reader = request.get("reader")
    val writer = request.get("writer")
    val jobName = Option(request.get("jobName", false)).getOrElse("datasysnc")
    val res = flinkxService.startJob(reader, writer, jobName)
    ("status" -> "0") ~ ("errstr" -> "") ~ ("result" -> res)
  }



  @POST
  @Path(value = "/query")
  @Operation(
    summary = "触发当前表导出任务",
    description = "触发当前导出任务",
    parameters = Array(
      new Parameter(
        name = "sql",
        in = ParameterIn.QUERY,
        description = "sql",
        required = true,
        array = new ArraySchema(
          schema = new Schema(implementation = classOf[String])
        )
      )
    )
  )
  @Produces(value = Array("application/json"))
  @Consumes(value = Array("application/json"))
  def query: Responder[JValue] = (request: HttpServletRequest) => {
    val sql = request.get("sql")
    val res = flinkxService.query(sql)
    ("status" -> "0") ~ ("errstr" -> "") ~ ("result" -> res)
  }

}

