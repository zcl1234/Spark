package com.tongji.cims.spark.session

import cn.tongji.cmis.constant.Constants
import cn.tongji.cmis.util.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * 自定义累加器
  * Created by 626hp on 2017/10/24.
  */

class SessionAggrStatAccumulator extends AccumulatorParam[String]{
  private val serialVersionUID: Long = 6311074555136039130L
  /**
    * 具体实现逻辑
    * */
  override def addInPlace(r1: String, r2: String): String = {
    add(r1,r2)
  }


  /**
    * 主要用于数据的初始化
    * */
  override def zero(initialValue: String): String = return Constants.SESSION_COUNT + "=0|" +
                                                             Constants.TIME_PERIOD_1s_3s + "=0|" +
                                                             Constants.TIME_PERIOD_4s_6s + "=0|" +
                                                             Constants.TIME_PERIOD_7s_9s + "=0|" +
                                                             Constants.TIME_PERIOD_10s_30s + "=0|" +
                                                             Constants.TIME_PERIOD_30s_60s + "=0|" +
                                                             Constants.TIME_PERIOD_1m_3m + "=0|" +
                                                             Constants.TIME_PERIOD_3m_10m + "=0|" +
                                                             Constants.TIME_PERIOD_10m_30m + "=0|" +
                                                             Constants.TIME_PERIOD_30m + "=0|" +
                                                             Constants.STEP_PERIOD_1_3 + "=0|" +
                                                             Constants.STEP_PERIOD_4_6 + "=0|" +
                                                             Constants.STEP_PERIOD_7_9 + "=0|" +
                                                             Constants.STEP_PERIOD_10_30 + "=0|"+
                                                             Constants.STEP_PERIOD_30_60 + "=0|" +
                                                             Constants.STEP_PERIOD_60 + "=0"
  private def add(v1:String,v2:String):String={
    if(StringUtils.isEmpty(v1))
      return v2
    val oldValue=StringUtils.getFieldFromConcatString(v1,"\\|",v2)
    if(oldValue!=null){
      val newValue=Integer.valueOf(oldValue)+1
      return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue))
    }
    return v1
  }

  override def addAccumulator(t1: String, t2: String): String = {
    add(t1,t2)
  }
}
