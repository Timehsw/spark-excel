package com.hsw.spark.excel.util

import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
object JudegExcelUtil {
  private val logger=LoggerFactory.getLogger(JudegExcelUtil.getClass)

//  判断是否是Excel文件.如果不是excel文件那么就直接退出了
  def isExcelFile(filePath:String):Boolean= {
  if (filePath==null|| !(isExcel2003(filePath)|| isExcel2007(filePath))) {
    false
  }else{
    true
  }
}

//  是否是2003的excel，返回true是2003
  def isExcel2003(filePath:String):Boolean=filePath.matches("^.+\\.(?i)(xls)$")

//  是否是2007的excel，返回true是2007
  def isExcel2007(filePath:String):Boolean=filePath.matches("^.+\\.(?i)(xlsx)$")

  def main(args: Array[String]) {
    val path="hdfs://192.168.4.202:8020/hsw/testfileDatafuse.xlsx"
    val flag=isExcelFile(path)
    println(flag)
    val flag1=isExcel2007(path)
    val flag2=isExcel2003(path)
    println(flag1)
    println(flag2)



  }
}
