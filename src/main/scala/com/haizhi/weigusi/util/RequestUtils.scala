package com.haizhi.weigusi.util

import javax.servlet.http.HttpServletRequest

/**
 * Created by jiangmeng on 2021-06-17.
 */
object RequestUtils {

  /**
   * http请求参数辅助类，简化参数处理
   * @param request
   */
  class RequestHelper(request: HttpServletRequest) {

    def get(param: String, isRequired: Boolean = true): String = {
      val data = request.getParameter(param)
      if (data == null && isRequired) {
        throw new Exception(s"Missing parameter : $param")
      }
      data
    }
  }

  implicit def toRequestHelper(request: HttpServletRequest): RequestHelper = new RequestHelper(request)
}
