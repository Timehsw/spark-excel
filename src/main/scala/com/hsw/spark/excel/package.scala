package com.hsw.spark

import com.hsw.spark.excel.util.ExcelFile
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.Map

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
package object excel {

  /**
    * Adds a method, `xmlFile`, to [[org.apache.spark.sql.SQLContext]] that allows reading XML data.
    * 运用隐士类的特性，给sqlContext类增加xmlFile方法
    */
  implicit class ExcelContext(sqlContext: SQLContext) extends Serializable {
    def excelFile(
                   filePath: String,
                   sheetNum: String = ExcelOptions.DEFAULT_SHEET_NUMBER, //解析第几个sheet页
                   isAllSheet: String = ExcelOptions.DEFAULT_ALL_SHEET, //是否解析所有的sheet页
                   useHeader: Boolean = ExcelOptions.DEFAULT_USE_HEADER, //是否把第一行当作结构去解析
                   delimiter: Char = ExcelOptions.DEFAULT_FIELD_DELIMITER.charAt(0), //默认分隔符
                   mode: String = ExcelOptions.DEFAULT_PARSE_MODE, //解析方式
                   charset: String = ExcelOptions.DEFAULT_CHARSET, // 字符编码
                   inferSchema: Boolean = ExcelOptions.DEFAULT_INFERSCHEMA //是否进行类型推断
                 ): DataFrame = {

      //      把excelFile的输入参数放入Map对象中.parameters里面包含所有的参数
      val parameters = Map(
        "delimiter" -> delimiter.toString,
        "sheetNumm" -> sheetNum,
        "isAllSheet" -> isAllSheet
      )

      val excelRelation = ExcelRelation(
        () => ExcelFile.withCharset(sqlContext.sparkContext, filePath, parameters.toMap, charset),
        location = Some(filePath),
        useHeader = useHeader,
        delimiter = delimiter,
        parseMode = mode,
        treatEmptyValuesAsNulls = true,
        inferExcelSchema = inferSchema
      )(sqlContext)
      sqlContext.baseRelationToDataFrame(excelRelation)

    }


    /**
      * 加一个方法,saveAsExcelFile.一个RDD能够被写到excel数据中
      * 如果compressionCodec不为null，所得到的输出将被压缩。
      * Note that a codec entry in the parameters map will be ignored.
      *
      * @param dataFrame
      */
    implicit class ExcelSchemaRDD(dataFrame: DataFrame) {
      def saveAsExcelFile(
                           path: String, parameters: Map[String, String] = Map(),
                           compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
        //        将dataframe中的数据存储到excel中
        ???
      }
    }

  }

}
