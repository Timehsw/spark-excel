package com.hsw.spark.excel

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by hushiwei on 2017/9/14.
  * desc : 
  */
trait ENV {
  var sc:SparkContext=_
  var sqlContext:SQLContext=_

}
