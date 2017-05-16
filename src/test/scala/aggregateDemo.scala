import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/2 0002.
  */
object aggregateDemo {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("aggregate").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val z=sc.parallelize(List(1,2,3,4,5,6),2)
//打印每个分区里面的内容
    z.glom().foreach(arr=>{
      println(arr.foldLeft(" ")((x,y)=>x+y))
    })

    // This example returns 16 since the initial value is 5 设置处理值5,注意这个初始值会出现在每个每个分区中作为初始值。就像下面的，每个分区都有5
    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
    // final reduce across partitions will be 5 + 5 + 6 = 16  进行最后一个方法的时候，还会加上初始值5
    // note the final reduce include the initial value 最后得出的才是结果
    // 因此得出结论，aggregate聚合算子的三个参数分别是。第一个是设置初始值。第二个Sep函数是对每个分区中的内容进行操作的函数。第三个Combi函数是聚合每个分区的函数
    val result=z.aggregate(0)(math.max(_,_),_+_)
    val result1=z.aggregate(5)(math.max(_,_),_+_)
    println("result: "+result)
    println("result1: "+result1)
//    456
//    123
//    result: 9
//    result1: 16
    sc.stop()
  }
}
