package com.hsw.spark.excel.util

import java.io.InputStream

import com.hsw.spark.excel.ExcelOptions
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, Workbook}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer, StringBuilder}


/**
  * Created by HuShiwei on 2016/7/29 0029.
  */

/**
  * 解析xls文件，利用poi对文件进行解析
  * Excel解析类，读取每一行并转成字符串输出以换行符分割，每一行的字段以\t分割，最终是一串字符串
  */
object ExcelParser {
  private val logger = LoggerFactory.getLogger(ExcelParser.getClass)
  private var currntString: StringBuilder = _
  private var workbook: Workbook=null
  def parseExcelData(is: InputStream,isExcel2003:Boolean,sheetNum:Int=ExcelOptions.DEFAULT_SHEET_NUMBER.toInt,isAllSheet:Boolean=true): Array[String] = {
    val resultList = new ListBuffer[String]()
//    判断是哪一种excel
    if (isExcel2003) {
      workbook=new HSSFWorkbook(is)
    }else{
      workbook=new XSSFWorkbook(is)
    }

    if (!isAllSheet) {
      val sheet = workbook.getSheetAt(sheetNum)
      val rowIterator = sheet.iterator()
      while (rowIterator.hasNext) {
        currntString = new StringBuilder()
        val row = rowIterator.next()
        val cellIterator = row.cellIterator()
        while (cellIterator.hasNext) {

          val cell = cellIterator.next()
          if (null != cell) {
            cell.getCellType match {
              case Cell.CELL_TYPE_BOOLEAN => {
                currntString.append(cell.getBooleanCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case Cell.CELL_TYPE_NUMERIC => {
                currntString.append(cell.getNumericCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case Cell.CELL_TYPE_STRING => {
                currntString.append(cell.getStringCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case Cell.CELL_TYPE_FORMULA => {
                currntString.append(cell.getCellFormula + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case Cell.CELL_TYPE_BLANK => {
                currntString.append("" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case Cell.CELL_TYPE_ERROR => {
                currntString.append("非法字符" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
              case _ => {
                currntString.append("未知类型" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
              }
            }
          }
        }
        resultList.append(currntString.toString())
      }
    }else {
      val shelltIt = workbook.sheetIterator()
      while (shelltIt.hasNext) {
        val sheet = shelltIt.next()
        val rowIterator = sheet.iterator()
        while (rowIterator.hasNext) {
          currntString = new StringBuilder()
          val row = rowIterator.next()
          val cellIterator = row.cellIterator()
          while (cellIterator.hasNext) {

            val cell = cellIterator.next()
            if (null != cell) {
              cell.getCellType match {
                case Cell.CELL_TYPE_BOOLEAN => {
                  currntString.append(cell.getBooleanCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case Cell.CELL_TYPE_NUMERIC => {
                  currntString.append(cell.getNumericCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case Cell.CELL_TYPE_STRING => {
                  currntString.append(cell.getStringCellValue + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case Cell.CELL_TYPE_FORMULA => {
                  currntString.append(cell.getCellFormula + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case Cell.CELL_TYPE_BLANK => {
                  currntString.append("" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case Cell.CELL_TYPE_ERROR => {
                  currntString.append("非法字符" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
                case _ => {
                  currntString.append("未知类型" + ExcelOptions.DEFAULT_FIELD_DELIMITER)
                }
              }
            }
          }
          resultList.append(currntString.toString())
        }

      }
    }

    is.close()
    return resultList.toArray
  }
}
