package com.hsw.spark.excel.util

import com.hsw.spark.excel.ExcelOptions
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
object ReadExcelToRdd {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("read excel").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val filePath: String = "hdfs://192.168.4.202:8020/hsw/testExcel.xls"

    val parameters=Map(
      "delimiter"->ExcelOptions.DEFAULT_FIELD_DELIMITER,
      "sheetNumm"->ExcelOptions.DEFAULT_SHEET_NUMBER,
      "isAllSheet"->ExcelOptions.DEFAULT_ALL_SHEET
    )
    val recelRDD=ExcelFile.withCharset(sc,filePath,parameters)
    recelRDD.foreach(println)
    sc.stop()
  }

}
