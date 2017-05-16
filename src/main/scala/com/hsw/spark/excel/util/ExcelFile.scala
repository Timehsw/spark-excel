package com.hsw.spark.excel.util

import java.nio.charset.Charset

import com.hsw.spark.excel.{ExcelInputFormat, ExcelOptions}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
private[excel] object ExcelFile {
  val DEFAULT_INDENT="  "
  val DEFAULT_ROW_SEPARATOR="\n"
  val DEFAULT_CHARSET=Charset.forName("UTF-8")

  def withCharset(
    context:SparkContext,
    location:String,
    parameters:Map[String,String],
    charset:String="utf-8"):RDD[String]={
    val options=ExcelOptions(parameters)
    context.hadoopConfiguration.set(ExcelInputFormat.ENCODING_KEY,charset)
    context.hadoopConfiguration.set(ExcelInputFormat.EXCEL_SHEET_NUMBER,options.sheetNumm)
    context.hadoopConfiguration.set(ExcelInputFormat.EXCEL_ALLSHEET,options.isAllSheet)
    context.hadoopConfiguration.set(ExcelInputFormat.DEFAULT_FIELD_DELIMITER,options.delimiter)

    if (Charset.forName(charset)==DEFAULT_CHARSET) {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength))
    } else {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }
}
