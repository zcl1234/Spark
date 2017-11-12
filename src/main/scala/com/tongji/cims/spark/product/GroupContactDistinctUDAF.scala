package com.tongji.cims.spark.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by 626hp on 2017/11/8.
  */
class GroupContactDistinctUDAF extends UserDefinedAggregateFunction{

  //指定输入数据的字段与类型
  var inputStream1=StructType(List(StructField("cityInfo",StringType,true)))
  //指定缓冲数据的字段与类型
  var bufferSchema1=StructType(List(StructField("bufferCityInfo",StringType,true)))

  var deterministic1=true

  override def inputSchema: StructType = {
    inputStream1
  }

  override def bufferSchema: StructType = {
    bufferSchema1
  }

  //指定返回类型
  override def dataType: DataType = {
    StringType
  }

  override def deterministic: Boolean =
  {
    deterministic1
  }

  /**
    * 在内部指定一个初始的值
    * */
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer.update(0,"")
  }
/**
  * 更新
  * 可以认为是，一个一个地将组内地字段值传递进来
  * 实现拼接的逻辑
  * */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo=buffer.getString(0)
    val cityInfo=input.getString(0)

    if(!bufferCityInfo.contains(cityInfo)) {
      if("".equals(bufferCityInfo)){
        bufferCityInfo+=cityInfo
      }else{
        bufferCityInfo+=","+cityInfo
      }
    }
    buffer.update(0,bufferCityInfo)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
  {
      var bufferCityInfo1=buffer1.getString(0)
      val bufferCityInfo2=buffer2.getString(0)

    for(cityInfo<-bufferCityInfo2.split(",")){
      if(!bufferCityInfo1.contains(cityInfo))
        {
          if("".equals(bufferCityInfo1)){
            bufferCityInfo1+=cityInfo
          }else{
            bufferCityInfo1+=","+cityInfo
          }
        }
    }
    buffer1.update(0,bufferCityInfo1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
