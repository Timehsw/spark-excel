package com.hsw.spark.excel

import java.io.InputStream
import java.nio.charset.Charset

import com.hsw.spark.excel.util.{ExcelParser, JudegExcelUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.slf4j.LoggerFactory


/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
class ExcelInputFormat extends TextInputFormat {
  private val logger = LoggerFactory.getLogger(ExcelInputFormat.getClass)

  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new ExcelRecordReader()
  }
}

object ExcelInputFormat {
  val ENCODING_KEY           : String = "excelinput.encoding"
  val EXCEL_SHEET_NUMBER     : String = "excelsheet.number"
  val EXCEL_ALLSHEET         : String = "excel.allsheet"
  val DEFAULT_FIELD_DELIMITER: String = "excel.fielddelimiter"

}

private[excel] class ExcelRecordReader extends RecordReader[LongWritable, Text] {

  val logger = LoggerFactory.getLogger("ExcelRecordReader.class")

  private var isExcel2003                  = true
  private var isAllSheetFlag: Boolean      = _
  private var sheetNum      : Int          = _
  private var currentKey    : LongWritable = _
  private var currentValue  : Text         = _

  private var start       : Long         = _
  private var end         : Long         = _
  private var in          : InputStream  = _
  private var filePosition: Seekable     = _
  private var decompressor: Decompressor = _

  private var strArrayofLine: Array[String] = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = {
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    val charset =
      Charset.forName(conf.get(ExcelInputFormat.ENCODING_KEY, ExcelOptions.DEFAULT_CHARSET))
    val sheet = conf.get(ExcelInputFormat.EXCEL_SHEET_NUMBER)
    val isAllSheet = conf.get(ExcelInputFormat.EXCEL_ALLSHEET)
    sheetNum = sheet.toInt
    isAllSheetFlag = isAllSheet.toBoolean
    logger.warn("open excel sheet number is: " + sheetNum)
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    //    打开文件并且找到这个split分片的起始位置
    val path = fileSplit.getPath
    val filePath = path.toString
    logger.warn("filePath: " + filePath)
    JudegExcelUtil.isExcelFile(filePath)
    //    验证文件的合法性
    if (!JudegExcelUtil.isExcelFile(filePath)) {
      logger.error("the file is not a excel file.the system will exit in a minute")
      sys.exit()
    }
    logger.warn("判断后是execl文件")
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)
    if (JudegExcelUtil.isExcel2007(filePath)) {
      isExcel2003 = false
    }
    strArrayofLine = ExcelParser.parseExcelData(fsin, isExcel2003, sheetNum, isAllSheetFlag)


    /*val codec=new CompressionCodecFactory(conf).getCodec(path)
    if (null!=codec) {
      decompressor=CodecPool.getDecompressor(codec)
      // Use reflection to get the splittable compression codec and stream. This is necessary
      // because SplittableCompressionCodec does not exist in Hadoop 1.0.x.
      def isSplitCompressionCodec(obj:Any)={
        val splittableClassName="org.apache.hadoop.io.compress.SplittableCompressionCodec"
        obj.getClass.getInterfaces.map(_.getName).contains(splittableClassName)
      }

      val (inputStream,seekable)=codec match {
        case c:CompressionCodec if isSplitCompressionCodec(c)=>
          val cIn = {
            val sc = c.asInstanceOf[SplittableCompressionCodec]
            sc.createInputStream(fsin, decompressor, start,
              end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
          }
          start=cIn.getAdjustedStart
          end=cIn.getAdjustedEnd
          (cIn,cIn)
        case c:CompressionCodec=>
          if (start!=0) {
            throw new IOException("Cannot seek in "+codec.getClass.getSimpleName+" compressed stream")

          }
          val cIn=c.createInputStream(fsin,decompressor)
          (cIn,fsin)
      }
      in=inputStream
      filePosition=seekable
    }else{
      in=fsin
      filePosition=fsin
      filePosition.seek(start)
    }*/

  }

  override def getProgress: Float = (filePosition.getPos - start) / (end - start).toFloat

  override def nextKeyValue(): Boolean = {
    if (currentKey == null) {
      currentKey = new LongWritable(-1)
      currentValue = new Text()
    } else {
      val pos = currentKey.get() + 1
      if (pos < strArrayofLine.length) {
        if (strArrayofLine(pos.asInstanceOf[Int]) != null) {
          currentKey.set(pos)
          currentValue.set(strArrayofLine(pos.asInstanceOf[Int]))
          return true
        }
      }
      return false
    }
    if (currentKey == null || currentValue == null) {
      return false
    } else {
      return true
    }
  }

  override def getCurrentValue: Text = currentValue


  override def getCurrentKey: LongWritable = currentKey

  override def close(): Unit = {
    try {
      if (in != null) {
        in.close()
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor)
        decompressor = null
      }
    }

  }
}
