package com.hsw.spark.excel.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.hsw.spark.excel.ExcelOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.control.Exception._

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
private[excel] object InferSchema {
  private val logger = LoggerFactory.getLogger(InferSchema.getClass)

  /**
    * 三个阶段,推断出excel记录中集合的类型：
    * 1.推断每一条记录的类型
    * 2.合并类型通过选择最低必需的类型去覆盖相同类型的key
    * 3.任何剩余的空字段替换为字符串
    */
  def apply(tokenRdd:RDD[Array[String]],
            header:Array[String],
            nullValue:String="",
            dateFormatter:SimpleDateFormat=null):StructType={
    val startType:Array[DataType]=Array.fill[DataType](header.length)(NullType)

//    为了推断出rootTypes这个字段的类型，主要用到了aggregate这个聚合算子
    val rootTypes:Array[DataType]=tokenRdd.aggregate(startType)(
      inferRowType(nullValue,dateFormatter),
      mergeRowTypes)

//    把第一行的字段们和之前推断出的类型们结合起来
    val structFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case z: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }

    StructType(structFields)

  }

//  推断这一行数据的真实类型。柯里化函数。
//  前面第一个括号中的两个参数是外部传进来的
//  后面这个括号中的两个参数注意，一个是startType从这里传进来的初始值。另外一个的类型就是RDD元素的类型。
  private def inferRowType(nullValue:String,dateFormatter:SimpleDateFormat)
                          (rowSoFar:Array[DataType],next:Array[String]):Array[DataType]= {
    var i = 0
    while (i<math.min(rowSoFar.length,next.length)) {//右边的列可能有缺少
      rowSoFar(i)=inferField(rowSoFar(i),next(i),nullValue,dateFormatter)
      i+=1
    }
    rowSoFar

  }

//  这是aggragate聚合函数的最后一个函数了。它接收第二个函数的返回值类型作为参数。对这进行两两聚合
  private[excel] def mergeRowTypes(
     first:Array[DataType],
     second:Array[DataType]):Array[DataType]={
    first.zipAll(second,NullType,NullType).map{case ((a,b))=>
    findTightestCommonType(a,b).getOrElse(NullType)}
  }

  /**
    * 推断字符串类型的字段的实际类型。给出已知的Double类型。一个字符串"1".检测出他是Int类型1是毫无意义的。最好的类型必须的Double或者更高的类型
    *
    * @param typeSoFar
    * @param field
    * @param nullValue
    * @param dateFormatter
    * @return
    */
  private def inferField(typeSoFar:DataType,
                         field:String,
                         nullValue:String="",
                         dateFormatter:SimpleDateFormat=null):DataType= {
    def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field)
    }

    def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDouble(field)
    }

    def tryParseDouble(field: String): DataType = if ((allCatch opt field.toDouble).isDefined) {
      DoubleType
    } else {
      tryParseTimestamp(field)
    }

    def tryParseTimestamp(field: String): DataType = {
      if (dateFormatter != null) {
        if ((allCatch opt dateFormatter.parse(field)).isDefined) {
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      } else {
        if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      }
    }

    def tryParseBoolean(field: String): DataType = {
      if ((allCatch opt field.toBoolean).isDefined) {
        BooleanType
      } else {
        stringType()
      }
    }

    def stringType(): DataType = {
      StringType
    }

    if (field==null || field.isEmpty||field==nullValue) {
      typeSoFar
    }else{
      typeSoFar match {
        case NullType => tryParseInteger(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case DoubleType => tryParseDouble(field)
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }

  private val numericPrecedence:IndexedSeq[DataType]=
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.Unlimited)

  val findTightestCommonType:(DataType,DataType)=>Option[DataType]={
    case (t1,t2) if t1==t2=>Some(t1)
    case (NullType,t1) =>Some(t1)
    case (t1,NullType) =>Some(t1)
    case (StringType,t2) =>Some(StringType)
    case (t1,StringType)=>Some(StringType)

    case (t1,t2) if Seq(t1,t2).forall(numericPrecedence.contains)=>
      val index=numericPrecedence.lastIndexWhere(t=>t==t1||t==t2)
      Some(numericPrecedence(index))

    case _=>None
  }

}
