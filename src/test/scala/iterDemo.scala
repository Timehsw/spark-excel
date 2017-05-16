/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
object iterDemo {
  def main(args: Array[String]) {
    val arr=Array[String]("aaaaaaaaa","bbbbbbbbbbbbbb","ccccccccccccccccc","hello")
    val it=arr.iterator
//    val str=it.filter(_!="hello")
//    str.foreach(println)
println("=========================")
    val ss=it.filter(line=>{
      println("=="+line)
      if (line!="hello") {
        true
      }else{
        false
      }
    })
    println("=========================")

    ss.foreach(x=>println("**"+x))
  }

}
