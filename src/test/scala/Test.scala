import RNG.SimpleRNG

object Test {

  def main(args: Array[String]) {
//    println(RNG.int(SimpleRNG(2)))
    val arr1=Array(1,2,3,4,5,6)
    val arr2=Array('a','b','c','d','e','f')
    arr1.zipAll(arr2,0,'c')
  }

}
