package com.hsw.spark.excel.util

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
private[excel] object ParserLibs {
  val OLD = "COMMONS"
  val NEW = "UNIVOCITY"

  val DEFAULT = OLD

  /**
    * 判断是否是可用的支持的解析库
    * @param lib
    * @return
    */
  def isValidLib(lib: String): Boolean = {
    lib.toUpperCase match {
      case OLD | NEW => true
      case _ => false
    }
  }

  /**
    * 判断是否是通用的解析库
    * @param lib
    * @return
    */
  def isCommonsLib(lib: String): Boolean = if (isValidLib(lib)) {
    lib.toUpperCase == OLD
  } else {
    true  // default
  }

  /**
    * 判断是否是单一的解析库
    * @param lib
    * @return
    */
  def isUnivocityLib(lib: String): Boolean = if (isValidLib(lib)) {
    lib.toUpperCase == NEW
  } else {
    false  // not the default
  }
}
