package com.haizhi.weigusi.util

import java.util.Properties

object DatasysncBuildInfo {

  val (
    version: String,
    branch: String,
    revision: String,
    buildUser: String,
    repoUrl: String,
    buildDate: String) = {

    val buildResource = "datasysnc-version-info.properties"
    val resourceStream = Thread.currentThread().getContextClassLoader.
      getResourceAsStream(buildResource)
    if (resourceStream == null) {
      throw new Exception(s"Could not find $buildResource")
    }

    try {
      val unknownProp = "<unknown>"
      val props = new Properties()
      props.load(resourceStream)
      (
        props.getProperty("version", unknownProp),
        props.getProperty("branch", unknownProp),
        props.getProperty("revision", unknownProp),
        props.getProperty("user", unknownProp),
        props.getProperty("url", unknownProp),
        props.getProperty("date", unknownProp)
      )
    } catch {
      case e: Exception =>
        throw new Exception(s"Error loading properties from $buildResource", e)
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close()
        } catch {
          case e: Exception =>
            throw new Exception("Error closing spark build info resource stream", e)
        }
      }
    }
  }
}
