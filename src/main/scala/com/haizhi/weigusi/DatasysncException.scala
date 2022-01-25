package com.haizhi.weigusi

/**
 * Created by jiangmeng on 2021-06-17.
 */
class DatasysncException(msg: String) extends Exception(msg)

class TassadarException(msg: String) extends DatasysncException(msg)
class MobiusException(msg: String) extends DatasysncException(msg)

class ServerException(status: Int, errMsg: String, cause: Throwable = null) extends Exception(errMsg, cause)
class RedisServerException(errMsg: String, cause: Throwable) extends  Exception(errMsg, cause)

object ServerStatusCode {
  val PARAMETERS_INVALID = 11
}
