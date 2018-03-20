package com.hsw.spark.excel

import com.hsw.spark.excel.util.{CompressionCodecs, ExcelFile, TypeCast}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/8/2 0002.
  */
/**
  * 自定义数据源从这个DefaultSource类开始看
  * 提供一个单纯的用SQL语句的方式去获取存储在Excel表格里的数据
  */
class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {
  private val logger = LoggerFactory.getLogger("DefaultSource===")


  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for EXCEL data."))
  }

  /**
    * 用给定的参数将存储在Excel中的数据创建一个新的relation
    * 参数中必须包含路径,可选的包括分隔符,引用的符号,还有是否把第一行作为结构
    *
    * @param sqlContext
    * @param parameters
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * 用给定的参数和用户提供的schema将存储在Excel中的数据创建一个新的relation
    * 参数中必须包含路径,可选的包括分隔符,引用的符号,还有是否把第一行作为结构
    *
    * @param sqlContext
    * @param parameters
    * @param schema
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    //检查路径
    val path = checkPath(parameters)

    logger.warn("=====================================" + parameters.toString())
    //    获取分隔符
    val delimiter = TypeCast.toChar(parameters.getOrElse("delimiter", ","))

    //    获取解析的模式.permissive是解析所有的行.droppmalformed删除不匹配的行
    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    //    获取header,判断是否把第一行数据当成结构,默认是false
    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag can be true or false")
    }

    val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "false")
    val treatEmptyValuesAsNullsFlag = if (treatEmptyValuesAsNulls == "false") {
      false
    } else if (treatEmptyValuesAsNulls == "true") {
      true
    } else {
      throw new Exception("Treat empty values as null flag can be true or false")
    }


    //    是否推断类型,默认是false

    val charset = parameters.getOrElse("charset", ExcelFile.DEFAULT_CHARSET.name())
    // TODO validate charset?
    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }

    //
    val nullValue = parameters.getOrElse("nullValue", "")

    val dateFormat = parameters.getOrElse("dateFormat", null)

    val codec = parameters.getOrElse("codec", null)

    /**
      * 重要的是这里了.我们继承的方法叫createRelation.因此就是要创建一个relation
      * 这个relation单例类.需要我们先去创建.继承他的父类BaseRelation,实现相关方法即可.
      * 然后在这里去调用这个方法就好了
      *
      * 值得注意的是第一个参数.惰性求值.需要用到的时候我们才求值
      * 第一个参数需要的是一个RDD.我们需要有一个类把我们的Excel数据读到分布式的RDD中
      * 因此我们重写了一个ExcelImportFormat
      * 剩下的参数,都是为了给这个普通的RDD赋予schema.最后成为一个DataFrame
      */
    ExcelRelation(
      () => ExcelFile.withCharset(sqlContext.sparkContext, path, parameters, charset),
      Some(path),
      headerFlag,
      delimiter,
      parseMode,
      treatEmptyValuesAsNullsFlag,
      schema,
      inferSchemaFlag,
      codec,
      nullValue,
      dateFormat)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      val codecClass = CompressionCodecs.getCodecClass(parameters.getOrElse("codec", null))
      //      data.saveAsCsvFile(path, parameters, codecClass)
      ???
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
