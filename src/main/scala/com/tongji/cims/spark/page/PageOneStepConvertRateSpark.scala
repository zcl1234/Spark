package com.tongji.cims.spark.page

import java.util
import java.util.{Collections, Comparator, Date}

import cn.tongji.cmis.constant.Constants
import cn.tongji.cmis.dao.IPageSplitConvertRateDAO
import cn.tongji.cmis.dao.factory.DAOFactory
import cn.tongji.cmis.domain.PageSplitConvertRate
import cn.tongji.cmis.util.{DateUtils, NumberUtils, ParamUtils, SparkUtils}
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.Map
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control._
import scala.collection.mutable.ListBuffer
/**
  * Created by 626hp on 2017/11/6.
  */
object PageOneStepConvertRateSpark {
  def main(args: Array[String]): Unit = {
      //1.构造spark上下文
    val conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE).set("spark.testing.memory", "2147480000")
    SparkUtils.setMaster(conf)

    val sc=new SparkContext(conf)
    val sqlContext=SparkUtils.getSQLContext(sc)
    //2.生成模拟数据
    SparkUtils.mockData(sc,sqlContext)
    //3.查询任务，获取任务参数
    val taskid=ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE)
    val taskDao=DAOFactory.getTaskDAO()
    val task=taskDao.findById(taskid)
    if (task == null) {
      System.out.println(new Date + ": cannot find this task with id [" + taskid + "].")
      return
    }
    val taskParam:JSONObject=JSON.parseObject(task.getTaskParam)
    //4.查询指定日期范围内的用户访问行为数据
    val actionRDD:RDD[Row]=SparkUtils.getActionRDDByDateRange(sqlContext,taskParam)

    val sessionid2actionRDD=actionRDD.map(x=>(x.getString(2),x))

    val sessionid2actionsRDD=sessionid2actionRDD.groupByKey()


    val pageSplitRDD=generateAndMatchPageSplit(sc,sessionid2actionsRDD,taskParam)
    val pageSplitPvMap=pageSplitRDD.countByKey()

    val startPagePv=getStartPagePv(taskParam,sessionid2actionsRDD)

     val convertRateMap=computePageSplitConvertRate(taskParam,pageSplitPvMap,startPagePv)

    persistConvertRate(taskid,convertRateMap)
  }
    //页面切片生成与匹配算法
  def generateAndMatchPageSplit(sc:SparkContext,sessionid2actionsRDD:RDD[(String,Iterable[Row])],taskParam:JSONObject):RDD[(String,Integer)]={
    val targetPageFlow=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowBroadcast=sc.broadcast(targetPageFlow)

    sessionid2actionsRDD.flatMap(x=>{
      val listBuffer=new ListBuffer[(String,Integer)]()
      val iterator=x._2.iterator
      val targerPageFlows=targetPageFlowBroadcast.value.split(",")

    //  val rowlist=new util.ArrayList[Row]()
      val rowlist=new ListBuffer[Row]()
      while(iterator.hasNext)
        {
          rowlist.append(iterator.next())
        }
      //使用隐式转换对list排序
      implicit val keyOrdering=new Ordering[Row]{
        override def compare(x: Row, y: Row): Int = {
          val actionTime1=x.getString(4)
          val actionTime2=y.getString(4)
          val date1=DateUtils.parseTime(actionTime1)
          val date2=DateUtils.parseTime(actionTime2)
          (date1.getTime-date2.getTime).toInt
        }
      }
      rowlist.sorted

      var lastPageid:Long=999999

      for(row<-rowlist)
        {
          var flag=true
          val pageid=row.getLong(3)
          if(lastPageid==999999){
            lastPageid=pageid
            flag=false             //flag的作用是起到continue的作用
          }
          //生成一个页面切片
          // 3,5,2,1,8,9
          // lastPageId=3
          // 5，切片，3_5
          if(flag) {
            val pageSplit = lastPageid + "_" + pageid
            val loop = new Breaks()
            loop.breakable {
              for (i <- 1 to targerPageFlows.length-1) {
                val targetPageSplit = targerPageFlows(i - 1) + "_" + targerPageFlows(i)
                if (pageSplit.equals(targetPageSplit)) {
                  listBuffer.append((pageSplit, 1))
                  loop.break()
                }
              }
            }
            lastPageid = pageid
          }
        }

      listBuffer
    })
  }
/**
  * 获取页面流中初始页面的pv
  * */
  def getStartPagePv(taskParam:JSONObject,sessionid2actionsRDD:RDD[(String,Iterable[Row])]): Long =
  {
    val targetPageFlow=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageid=targetPageFlow.split(",")(0).toLong
    val startPageRDD=sessionid2actionsRDD.flatMap(x=>{
      val listBuffer=new ListBuffer[Long]()
      val iterator=x._2.iterator
      while(iterator.hasNext)
        {
          val row=iterator.next()
          if(row.getLong(3)==startPageid)
            {
              listBuffer.append(row.getLong(3))
            }
        }
      listBuffer
    })
    startPageRDD.count()
  }
/**
  * 计算页面切片转化率
  * */

  def computePageSplitConvertRate(taskParam:JSONObject, pageSplitPvMap:Map[String,Long], startPagePv:Long):Map[String,Double] ={

    val convertRateMap=new mutable.HashMap[String,Double]()
    val targetPages=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",")

    var lastPageSplitPv:Long=0
    for(i<-1 to targetPages.length-1){
      val targetPageSplit=targetPages(i-1)+"_"+targetPages(i)
      val targetPageSplitPv=pageSplitPvMap(targetPageSplit)
      var convetedRate=0.0

      if(i==1)
        {
          convetedRate=NumberUtils.formatDouble(targetPageSplitPv.toDouble/startPagePv.toDouble,2)
        }else{
        convetedRate=NumberUtils.formatDouble(targetPageSplitPv.toDouble/lastPageSplitPv.toDouble,2)
      }
      convertRateMap.put(targetPageSplit,convetedRate)
      lastPageSplitPv=targetPageSplitPv
    }
    convertRateMap
  }
  /**
    * 持久化转化率
    * */
  def persistConvertRate(taskid:Long,convertRateMap:Map[String,Double]): Unit =
  {
    val stringBuffer=new StringBuffer()
    for(convertRateEntry<-convertRateMap){
      val pageSplit=convertRateEntry._1
      val convertRate=convertRateEntry._2
      stringBuffer.append(pageSplit+"="+convertRate+"|")
    }

    val convertRate=stringBuffer.toString
    val pageSplitConvertRate=new PageSplitConvertRate()
    pageSplitConvertRate.setTaskid(taskid)
    pageSplitConvertRate.setConvertRate(convertRate)

    val pageSplitConvertRateDAO: IPageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    pageSplitConvertRateDAO.insert(pageSplitConvertRate)
  }


}
