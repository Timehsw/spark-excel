package com.hsw.spark.excel

import java.io.IOException
import java.text.SimpleDateFormat

import com.hsw.spark.excel.readers.LineExcelReader
import com.hsw.spark.excel.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */

/**
  * protected[spark] ExcelRelation只在spark包下被访问
  * ExcelRelation 是case class 意味着使用它的时候可以不需要new ,并且case class 可以带参数，但是object是不可以带参数的
  * () => 这是非严格求值的写法
  */
case class ExcelRelation protected[excel](
   baseRDD: () => RDD[String],//这里很重要.意味着我们首先需要有一个RDD.
   location: Option[String],
   useHeader: Boolean,
   delimiter: Char,
   parseMode: String,
   treatEmptyValuesAsNulls: Boolean,
   userSchema: StructType = null,
   inferExcelSchema: Boolean,
   codec: String = null,
   nullValue: String = "",
   dateFormat: String = null)(@transient val sqlContext:SQLContext)
  extends BaseRelation
  with InsertableRelation
  with TableScan //把所有的Row对象全部包括到RDD中
  with PrunedScan { // 把所有的Row对象中,消除不需要的列,然后包括到RDD中

  private val logger = LoggerFactory.getLogger(ExcelRelation.getClass)

//  解析时间类型
  private val dateFormatter= if (dateFormat!=null) {
    new SimpleDateFormat(dateFormat)
  }else{
    null
  }

//  判断是否是FAIL_FAST_MODE解析模式,这个快速失败模式是 如果遇到一个畸形的行内容,造成了异常的出现,那么就终止了
  private val failFast = ParseModes.isFailFastMode(parseMode)
  //  判断是否是DROP_MALFORMED_MODE解析模式,这个模式是 当你有的数据比你希望拿到的数据多了或者少了的时候,造成了不匹配这个schema的时候,就删除这些行
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  //  判断是否是PERMISSIVE_MODE解析模式,这个模式是 尽量去解析所有的行.丢失值插入空值和多余的值将被忽略
  private val permissive = ParseModes.isPermissiveMode(parseMode)

//  推断RDD的类型类
  override val schema: StructType = inferSchema()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val filesystemPath = location match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.

      val codecClass = CompressionCodecs.getCodecClass(codec)
    /*  data.saveAsExcelFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString),
        codecClass)*/
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }

//  生成一个Row类型的RDD
  override def buildScan(): RDD[Row] = {
    val simpleDateFormatter=dateFormatter
    val schemaFileds=schema.fields
    val rowArray=new Array[Any](schemaFileds.length)
    tokenRdd(schemaFileds.map(_.name)).flatMap{ tokens=>

      if (schemaFileds.length!=tokens.length) {
        logger.warn(s"Dropping lines:${tokens.mkString(",")}")
        None
      }else{
        var index:Int=0
        try{
          index=0
          while (index<schemaFileds.length) {
            val field=schemaFileds(index)
            rowArray(index)=TypeCast.castTo(tokens(index),field.dataType,field.nullable,
              treatEmptyValuesAsNulls,nullValue,simpleDateFormatter)
            index=index+1
          }
          Some(Row.fromSeq(rowArray))
        }catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFileds.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case _: java.lang.NumberFormatException |
               _: IllegalArgumentException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
        }
      }
    }
  }

  /**
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields

    //    判断是否需要重新取数据
    val shouldTableScan = schemaFields.deep == requiredFields.deep

    val safeRequiredFields = if (dropMalformed) {
      //      如果是dropMalformend模式，那么需要去解析所有的值
      //      因此我们需要去决定哪些行是畸形的
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    }else{
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.length) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          } else if (permissive && schemaFields.length < tokens.length) {
            tokens.take(schemaFields.length)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls,
                nullValue,
                simpleDateFormatter
              )
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case _: java.lang.NumberFormatException |
                 _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def inferSchema():StructType= {
    if (userSchema!=null) {
      userSchema
    }else {
      val firstRow =
        new LineExcelReader(
          fieldSep = delimiter
        ).parseLine(firstLine)
      //      是否把第一行当作结构
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
//      对字段进行类型推断
      if (this.inferExcelSchema) {
        val simpleDateFormatter=dateFormatter
        InferSchema(tokenRdd(header),header,nullValue,simpleDateFormatter)
      }else{
//        不对类型进行类型推断，那么默认就都是StringType类型
        val schemaFields=header.map{fieldName=>
        StructField(fieldName.toString,StringType,nullable=true)}
      StructType(schemaFields)
      }
    }
  }

  /**
    * 从RDD中取出第一行无空的数据
    */
  private lazy val firstLine= {
      baseRDD().filter{line=>
      line.trim.nonEmpty}.first()
  }

//  取数据,根据参数判断是否需要过滤与第一行相同的行
  private def tokenRdd(header:Array[String]):RDD[Array[String]]={

    val filterLine= if (useHeader) firstLine else null

//    如果设置了header,在发送到其他的executors之前确保第一行已经序列化过了
    baseRDD().mapPartitions{iter=>
    val excelIter= if (useHeader) {
      iter.filter(_!=filterLine)
    }else{
      iter
    }
      parseEXCEL(excelIter)
    }
  }

//  把每一行的String内容,分割后把每个字段存储到Array中了
  private def parseEXCEL(iter:Iterator[String]):Iterator[Array[String]]={
    iter.flatMap {line=>
      val delimiter=ExcelOptions.DEFAULT_FIELD_DELIMITER
      val records=line.split(delimiter).toList

      if (records.isEmpty) {
        logger.warn(s"Ignoring empty line:$line")
        None
      }else{
        Some(records.toArray)
      }

    }
  }

}
