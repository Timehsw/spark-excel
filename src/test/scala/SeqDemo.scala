import org.apache.spark.sql.types.{StringType, _}

/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
object SeqDemo {
  def main(args: Array[String]) {
    val numericPrecedence:IndexedSeq[DataType]= IndexedSeq[DataType](
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

//        把tuple中的两个元素放在Seq序列中，然后循环序列中的每一个元素。拿到SQL类型中去遍历判断。
      case (t1,t2) if Seq(t1,t2).forall(numericPrecedence.contains)=>
        val index=numericPrecedence.lastIndexWhere(t=>t==t1||t==t2)
        println(index)
        Some(numericPrecedence(index))

      case _=>None
    }

    val t=findTightestCommonType(TimestampType,DoubleType)
    print(t)
  }

}
