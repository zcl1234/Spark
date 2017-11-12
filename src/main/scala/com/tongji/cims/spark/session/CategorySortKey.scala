package com.tongji.cims.spark.session

/**
  * Created by 626hp on 2017/10/25.
  */
class CategorySortKey(val clickcount:Long,val ordercount:Long,val paycount:Long) extends Ordered[CategorySortKey] with Serializable{
  override def compare(that: CategorySortKey): Int = {
    if(clickcount==that.clickcount)
      {
        return (ordercount-that.ordercount).toInt
      } else if(ordercount==that.ordercount)
      {
        return (paycount-that.paycount).toInt
      }

    return (clickcount-that.clickcount).toInt
  }
}
