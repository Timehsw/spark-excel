package com.hsw.spark.excel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

/**
  * Created by hushiwei on 2017/9/12.
  * desc : dsp广告投放单元测试类
  */
class ScalaFunc extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {
  override def beforeAll(): Unit = {
    super.beforeAll()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    sc = new SparkContext(new SparkConf()
      .setAppName("spark excel test")
      .setMaster("local[*]")
    )
    sqlContext = new SQLContext(sc)
  }

  test("read excel simple") {

    val filePath: String = "src/main/resources/testExcel.xls"

    val excelDF = sqlContext.excelFile(filePath)

    excelDF.printSchema()
    excelDF.show()
    sc.stop()


  }


  test("read excel") {

    val filePath: String = "hdfs://U007:8020/user/hsw/testExcel.xls"
    //    val filePath: String = "src/main/resources/testExcel.xls"

    val excelDF = sqlContext.excelFile(filePath, sheetNum = "0", isAllSheet = "false", inferSchema = true)
    //    val excelDF=sqlContext.excelFile(filePath)
    excelDF.printSchema()
    excelDF.show()
    //    excelDF.registerTempTable("user")
    //    sqlContext.sql("select name from user").show()
    sc.stop()


  }


  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }


}
