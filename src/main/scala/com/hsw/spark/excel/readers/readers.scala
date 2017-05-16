package com.hsw.spark.excel.readers

import com.hsw.excel.ExcelParser
import com.hsw.spark.excel.ExcelOptions

/**
  * Created by HuShiwei on 2016/8/2 0002.
  */

private abstract class ExcelReader(
  fieldSep: Char = ',',
  lineSep: String = "\n",
  quote: Char = '"',
  escape: Char = '\\',
  commentMarker: Char = '#',
  ignoreLeadingSpace: Boolean = true,
  ignoreTrailingSpace: Boolean = true,
  headers: Seq[String],
  inputBufSize: Int = 128,
  maxCols: Int = 20480) {
  protected lazy val parser: ExcelParser = {
    ???
  }
}

private[excel] class LineExcelReader(
  fieldSep: Char = ','){
  def parseLine(line:String):Array[String]={
    val delimiter=ExcelOptions.DEFAULT_FIELD_DELIMITER
    line.split(delimiter)
  }
}

