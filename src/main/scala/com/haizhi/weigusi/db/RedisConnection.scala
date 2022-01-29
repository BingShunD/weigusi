package com.haizhi.weigusi.db

import com.haizhi.weigusi.util.DatasysncConf
import com.lambdaworks.redis.{RedisClient, RedisConnection, RedisConnectionPool, RedisURI}

object RedisConnection {

  private val redisUri:RedisURI =
    RedisURI.Builder
      .redis(DatasysncConf.redisHost,DatasysncConf.redisPort)
      .withPassword(DatasysncConf.redisPassword)
      .build()

  private val client:RedisClient = RedisClient.create(redisUri)

  def redisClient: RedisClient = {
    client
  }

  def redisConnection: RedisConnectionPool[RedisConnection[String, String]] = {
    client.pool()
  }

}
