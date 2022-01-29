package com.haizhi.weigusi.service

import com.haizhi.weigusi.db.RedisConnection
import com.lambdaworks.redis.protocol.SetArgs

object RedisService {

   private val connectionPoll = RedisConnection.redisConnection
   private val connection= connectionPoll.allocateConnection()

  def exist(key: String*): Boolean = {
    connection.exists(key: _*) > 0
  }

  def setValue(key: String, value: String, expire: Int = -1): String = {
    if (expire > 0) {
      connection.set(key, value, SetArgs.Builder.ex(expire.asInstanceOf[Long]))
    } else {
      connection.set(key, value)
    }
  }

  def addToSet(key: String, value: String): Long =
    connection.sadd(key, value)

  def removeFromSet(key: String, value: String): Long =
    connection.srem(key, value)

  def membersOfSet(key: String): java.util.Set[String] = {
    connection.smembers(key)
  }

  def memberInSet(key: String, value: String): Boolean =
    connection.sismember(key, value)

  def pushToList(key: String, value: String): Long =
    connection.rpush(key, value)

  def removeFromList(key: String, value: String): Long =
    connection.lrem(key, -1, value)

  def delKey(key: String): Long =
    connection.del(key)

  def getValue(key: String): Option[String] =
    Option(connection.get(key))

  def putValue(key: String, value: String): Long =
    connection.lpush(key, value)

  def publish(channel: String, value: String): Long =
    connection.publish(channel, value)

  def unLock(key: String): Long =
    connection.del(key)

  def ttl(key: String): Long = {
    connection.ttl(key)
  }

  def lock(key: String, expireTime: Long): Unit = {
    var retryCount = 0
    while (retryCount <= expireTime) {
      if (retryCount == expireTime) {
        connection.del(key)
      }
      val res = connection.setnx(key, "mindIfIStay")
      if (res) {
        connection.expire(key, expireTime / 1000)
        return
      } else {
        retryCount = retryCount + 1000
        Thread.sleep(1000)
      }
    }
  }

  def lock(key: String, value : String, expireTime: Long): Unit = {
    var retryCount = 0
    while (retryCount <= expireTime) {
      if (retryCount == expireTime) {
        connection.del(key)
      }
      val res = connection.setnx(key, value)
      if (res) {
        connection.expire(key, expireTime / 1000)
        return
      } else {
        retryCount = retryCount + 1000
        Thread.sleep(1000)
      }
    }
  }

  def wait(key: String, values : Set[String], expireTime: Long): (Boolean, String) = {
    var retryCount = 0
    while (retryCount <= expireTime) {
      val res = connection.get(key)
      if (res == null || values.contains(res)) {
        return (true, res)
      } else {
        retryCount = retryCount + 1000
        Thread.sleep(1000)
      }
    }
    (false, key)
  }

  def incrby(key: String, value: Long): Unit = {
    connection.incrby(key, value)
  }

  def expire(key: String, expireTime: Long): Unit = {
    connection.expire(key, expireTime)
  }


}
