package com.hsw.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
object ReExcel {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("read excel").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
//    val filePath: String = "hdfs://ncp161:8020/hsw/testExcel.xls"
//    val filePath: String = "hdfs://ncp161:8020/hsw/jusfounDatafuse.xls"
    val filePath: String = "hdfs://master:8020/user/husw/jusfounDatafuse.xls"
//    val filePath: String = "hdfs://ncp161:8020/hsw/allSheet.xlsx"

    val excelDF=sqlContext.excelFile(filePath,sheetNum="1",isAllSheet = "false",inferSchema=true)
//    val excelDF=sqlContext.excelFile(filePath)
    excelDF.printSchema()
    excelDF.show()
//    excelDF.registerTempTable("user")
//    sqlContext.sql("select name from user").show()
    sc.stop()

  }

}
