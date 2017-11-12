import java.util.Date

import scala.collection.mutable

/**
  * Created by 626hp on 2017/10/25.
  */
object test {
  def main(args: Array[String]): Unit = {
    val testMap=new mutable.HashMap[String,String]()
    testMap.put("a","1")
    testMap.put("b","1")
    for(test<-testMap)
      {
        println(test._1+":"+test._2)
      }
    println(new Date().getTime)

  }
}
