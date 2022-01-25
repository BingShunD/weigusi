package com.haizhi.weigusi.util

import java.io.{PrintWriter, StringWriter}

object ExceptionUtils {

  class ExceptionHelper(e: Throwable) {

    def detailMessage(): String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      e.printStackTrace(pw)
      sw.toString
    }
  }

  implicit def toExceptionHelper(e: Throwable): ExceptionHelper = new ExceptionHelper(e)
}

