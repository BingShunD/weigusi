package com.haizhi.weigusi.util

import java.io.IOException
import java.security.KeyStore

import scala.collection.JavaConverters._

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.ssl.SSLContexts
import org.apache.spark.sql.{DataFrame, Row}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.{IndicesOptions, WriteRequest}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders, TermQueryBuilder}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest, UpdateByQueryAction, UpdateByQueryRequest}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._



object ESClientConnectionUtil extends LazyLogging {
  implicit val formats = DefaultFormats

  def getClient(host: String, port: String, esSecurity: Boolean = false,
                username: String = null, password: String = null, esNetSsl: Boolean = false ): RestHighLevelClient = {
    if (!esSecurity) {
      new RestHighLevelClient(RestClient.builder(new HttpHost(host, port.toInt, "http")))
    } else if (!esNetSsl) {
      getSecurityClient(host, port, username, password)
    } else {
      getSecurityClientSSL(host, port, username, password)
    }
  }

  def getSecurityClient(host: String, port: String, username: String, password: String): RestHighLevelClient = {
    val  credentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))
    val httpHosts = new HttpHost(host, port.toInt, "http")
    val builder = RestClient.builder(httpHosts)
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          httpClientBuilder.disableAuthCaching
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      })
    new RestHighLevelClient(builder)
  }


  def getSecurityClientSSL(host: String, port: String, username: String, password: String): RestHighLevelClient = {
    val credentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))


    val httpHosts = new HttpHost(host, port.toInt, "https")

    val custom = SSLContexts.custom()
    custom.loadTrustMaterial(null: KeyStore, null)
    val sslContext = SSLContexts.custom.build

    val builder = RestClient.builder(httpHosts)
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
          httpClientBuilder.setSSLContext(sslContext)
        }
      })
    new RestHighLevelClient(builder)
  }

  def getMax(idxName: String, idxType: String, filed: String, host: String, port: String, esSecurity: Boolean = false,
             username: String = null, password: String = null, esNetSsl: Boolean = false ) : Option[String] = {
    val client = getClient(host, port, esSecurity, username, password, esNetSsl)
    val res = try {
      val sourceBuilder = new SearchSourceBuilder
      val agg = AggregationBuilders.max(s"max${filed}").field(filed)
      sourceBuilder.aggregation(agg)
      val rq = new SearchRequest(idxName)
      rq.indices(idxName).types(idxType)
      rq.source(sourceBuilder)
      val response = client.search(rq, RequestOptions.DEFAULT)
      Option(response.getAggregations().get(s"max${filed}").asInstanceOf[Max].getValueAsString)
    } catch {
      case e: IOException =>
        e.printStackTrace()
        None
    } finally {
      if (client != null) client.close()
    }
    res
  }

  def createIndex(idxName: String, esType: String, mapping: String, host: String, port: String,
                  esSecurity: Boolean = false, username: String = null, password: String = null,
                  esNetSsl: Boolean = false ): Unit = {
    try {
      val client = getClient(host, port, esSecurity, username, password, esNetSsl)
      try {

        logger.info(s"idxName = ${idxName}, mapping = ${mapping}")

        val parseJvalue = parse(mapping)
        val request = new CreateIndexRequest(idxName)
        val jsonSetting = write( parseJvalue \ "settings")
        request.settings(jsonSetting, XContentType.JSON)
        val jsonMapping = write(parseJvalue \ "mappings")
        request.mapping(esType, jsonMapping, XContentType.JSON)
        val createIndexResponse = client.indices.create(request)
        val acknowledged = createIndexResponse.isAcknowledged
        val shardsAcknowledged = createIndexResponse.isShardsAcknowledged
        if (!acknowledged && !shardsAcknowledged)
        {
          throw new Exception("索引创建失败")
        }
      } catch {
        case e: IOException =>
          e.printStackTrace()
      } finally if (client != null) client.close()
    }
  }

  def deleteIndex(idxName: String, host: String, port: String, esSecurity: Boolean = false, username: String = null,
                  password: String = null, esNetSsl: Boolean = false): Unit = {
    try {
      val client = getClient(host, port, esSecurity, username, password, esNetSsl)
      try {
        val request = new DeleteIndexRequest(idxName)
        request.timeout(TimeValue.timeValueMinutes(2))
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1))
        request.indicesOptions(IndicesOptions.lenientExpandOpen)
        val deleteIndexResponse = client.indices.delete(request, RequestOptions.DEFAULT)
        if (!deleteIndexResponse.isAcknowledged)
        {
          throw new Exception("删除索引失败！")
        }
      } catch {
        case e: IOException =>
          e.printStackTrace()
      } finally if (client != null) client.close()
    }
  }

  def deleteQuery(list: List[String], idxName: String, host: String, port: String, esSecurity: Boolean = false,
                  username: String = null, password: String = null, esNetSsl: Boolean = false): Long = {
    val client = getClient(host, port, esSecurity, username, password, esNetSsl)
    // 参数为索引名，可以不指定，可以一个，可以多个
    val request = new DeleteByQueryRequest(idxName)
    // 更新时版本冲突
    request.setConflicts("proceed")
    // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
    val qb = QueryBuilders.termsQuery("_id", list.asJavaCollection)
    request.setQuery(qb)
    // request.setQuery();
    // 更新最大文档数
    // 批次大小
    request.setBatchSize(10000)
    // 并行
    val slices = if (list.length > 100000) 10 else list.length / 10000 + 1
    request.setSlices(slices)
    // 使用滚动参数来控制“搜索上下文”存活的时间
    request.setScroll(TimeValue.timeValueMinutes(10))
    // 超时
    request.setTimeout(TimeValue.timeValueMinutes(2))
    // 刷新索引
    request.setRefresh(true)
    try {
      val response = client.deleteByQuery(request, RequestOptions.DEFAULT)
      return response.getStatus.getUpdated
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally try
      client.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    -1
  }

  def modifyQuery (
                    idxName: String,
                    host: String,
                    port: String,
                    esId: String,
                    df: DataFrame,
                    model: String,
                    esSecurity: Boolean = false,
                    username: String = null,
                    password: String = null,
                    esNetSsl: Boolean = false): Unit = {
    val rdd = df.rdd
    for (partition <- rdd.partitions) {
      val rows = rdd.mapPartitionsWithIndex(getParValue(_, _, partition.index), true).collect()
      if (rows.nonEmpty) {
        val client = getClient(host, port, esSecurity, username, password, esNetSsl)
        val fields = df.schema.fields.toSeq.map(_.name)
        val request = new BulkRequest()
        request.timeout(TimeValue.timeValueMinutes(2))
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        request.waitForActiveShards(2)

        try {
          rows.foreach(row => {
            val values = scala.collection.mutable.Map[String, Any]()
            val id = row.getAs[String](esId)
            for (i <- 0 until row.length) {
              values.put(fields(i), row.get(i))
            }
            if (model == "append") {
              request.add(new IndexRequest(idxName, idxName, id).source(values.asJava))
            } else if (model == "update") {
              request.add(new UpdateRequest(idxName, idxName, id).doc(values.asJava))
            }
          })
          client.bulk(request, RequestOptions.DEFAULT)
        } catch {
          case e: IOException =>
            e.printStackTrace()
        } finally try
          client.close()
        catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }

  def getParValue(idx: Int, ds: Iterator[Row], currentPar: Int): Iterator[Row] = {
    if (currentPar == idx) ds else Iterator()
  }

}

