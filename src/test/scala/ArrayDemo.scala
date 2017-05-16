/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
/*
 描述:
返回一个array，该array的元素由当前序列与that序列对应位置元素组成的pair。如果当前序列与that序列长度不同，则长度短的那个序列会根据指定的元素进行填充，直到两个序列的长度相等为止。
参数说明:
that ：用于拉链操作的另一个序列。
thisElem ：当前序列比that序列较短时，该元素用于填充当前序列使用。
thatElem ：当that序列比当前序列较短时，该元素用于填充that序列使用。

返回值:
一个数组，数组的元素为当前序列与that序列对应位置元素组成的pair。数组的长度为两个序列中长度较大的那个序列的长度。
  */
object ArrayDemo {
  def main(args: Array[String]) {
    val arr1 = Array[String]("1", "2")
    val arr2 = Array[String]("1", "bbbb", "aaa", "ccc")
    val arr = arr1.zipAll(arr2, "bbbb", "aaa")
    val it = arr.iterator
    while (it.hasNext) {
      val d = it.next()
      println(d._1 + d._2)

    }


  }

}
