package com.hsw.spark.excel

import org.apache.spark.sql.sources.DataSourceRegister

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
class DefaultSource15 extends DefaultSource with DataSourceRegister {
  /**
    * 给spark-excel数据源设置别名
    *
    * @return
    */
  override def shortName(): String = "excel"
}
