package com.hsw.spark.excel.util

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Locale

import org.apache.spark.sql.types.{DateType, TimestampType, _}

import scala.util.Try

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
/**
  * 实用的类型转换方法
  */
object TypeCast {
  /**
    * 把给定的string类型转成指定的数据类型
    *
    *
    */
  private[excel] def castTo(
    datum:String,
    castType:DataType,
    nullable:Boolean=true,
    treatEmptyValuesAsNulls:Boolean=false,
    nullValue:String="",
    dateFormatter:SimpleDateFormat=null):Any= {
    //如果空值不是空的字符串,就不需要treateEmptyValuesAsNulls
    //设置成true
    val nullValueIsNotEmpty = nullValue != ""
    if (datum == nullValue && nullable
      && (!castType.isInstanceOf[StringType] || treatEmptyValuesAsNulls || nullValueIsNotEmpty)) {
      null
    } else {
      castType match {
        case _: ByteType => datum.toByte
        case _: ShortType => datum.toShort
        case _: IntegerType => datum.toInt
        case _: LongType => datum.toLong
        case _: FloatType => Try(datum.toFloat)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
        case _: DoubleType => Try(datum.toDouble)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
        case _: BooleanType => datum.toBoolean
        case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
        case _: TimestampType if dateFormatter != null =>
          new Timestamp(dateFormatter.parse(datum).getTime)
        case _: TimestampType => Timestamp.valueOf(datum)
        case _: DateType if dateFormatter != null =>
          new Date(dateFormatter.parse(datum).getTime)
        case _: DateType => Date.valueOf(datum)
        case _: StringType => datum
        case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }
    @throws[IllegalArgumentException]
    private[excel] def toChar(str:String):Char={
      if (str.charAt(0) == '\\') {
        str.charAt(1)
        match {
          case 't' => '\t'
          case 'r' => '\r'
          case 'b' => '\b'
          case 'f' => '\f'
          case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
          case '\'' => '\''
          case 'u' if str == """\u0000""" => '\u0000'
          case _ =>
            throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
        }
      } else if (str.length == 1) {
        str.charAt(0)
      } else {
        throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
      }
  }
}
