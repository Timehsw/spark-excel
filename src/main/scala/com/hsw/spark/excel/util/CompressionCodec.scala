package com.hsw.spark.excel.util

import org.apache.hadoop.io.compress._

import scala.util.control.Exception._

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
/**
  * 从hdfs上读取excel文件的时候,支持的压缩类型为bzip2,gzip,lz4,snappy
  */
private[excel] object CompressionCodecs {

  private val shortCompressionCodecNames:Map[String,String]={
    val codecMap=collection.mutable.Map.empty[String, String]
    allCatch toTry(codecMap += "bzip2" -> classOf[BZip2Codec].getName)
    allCatch toTry(codecMap += "gzip" -> classOf[GzipCodec].getName)
    allCatch toTry(codecMap += "lz4" -> classOf[Lz4Codec].getName)
    allCatch toTry(codecMap += "snappy" -> classOf[SnappyCodec].getName)
    codecMap.toMap
  }
  /**
    * 用给定的名字返回codec class
    */
  def getCodecClass:String=>Class[_<:CompressionCodec]={
    case null => null
    case codec=>
      val codecName=shortCompressionCodecNames.getOrElse(codec.toLowerCase,codec)
      try{
        Class.forName(codecName).asInstanceOf[Class[CompressionCodec]]
      }catch {
        case e:ClassNotFoundException=>
          throw new IllegalArgumentException(s"Codec [$codecName] is not "+
          s"available. Known codecs are ${shortCompressionCodecNames.keys.mkString(", ")}.")
      }

  }

}
