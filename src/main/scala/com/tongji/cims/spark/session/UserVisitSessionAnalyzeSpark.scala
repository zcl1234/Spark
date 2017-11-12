package com.tongji.cims.spark.session

import java.util
import java.util.{Date, Random}

import cn.tongji.cmis.conf.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext}
import cn.tongji.cmis.constant.Constants
import cn.tongji.cmis.dao.{ISessionDetailDAO, ISessionRandomExtractDAO, ITaskDAO, ITop10SessionDAO}
import cn.tongji.cmis.dao.factory.DAOFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import cn.tongji.cmis.test.MockData
import cn.tongji.cmis.util._
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.Accumulator
import cn.tongji.cmis.domain._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * Created by 626hp on 2017/10/23.
  */
class  UserVisitSessionAnalyzeSpark extends Serializable{
   def getSQLContext(sc:SparkContext):SQLContext={
    val local:Boolean=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      return new SQLContext(sc)
    }else{
      return new HiveContext(sc)
    }
  }
  def mockData(sc:SparkContext,sQLContext:SQLContext){
    val local:Boolean=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local)
      {
        MockData.mock(sc,sQLContext)
      }
  }

  /**
    * 获取指定日期范围内的用户访问行为数据
    * */
  def getActionRDDByDateRange(sQLContext:SQLContext,taskParam:JSONObject):RDD[Row]={
    val startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val sql: String = "select * " + "from user_visit_action " + "where date>='" + startDate + "' " + "and date<='" + endDate + "'"
    val actionDF:DataFrame=sQLContext.sql(sql)
    //这里可能发生的问题：
    //SparkSql可能默认就给第一个stage设置了20个task 但是根据数据量以及算法的复杂度 实际上，可能需要1000个task去执行
    //所以 这里可以对SparkSQL刚刚查询出来的RDD执行repartition重分区操作
    // return actionDF.rdd.repartiton(1000)
    actionDF.rdd
  }

  /**
    * 获取session到访问行为数据的映射RDD
    * */
  def getSessionid2ActionRDD(actionRDD:RDD[Row]): RDD[(String,Row)] ={
       //   actionRDD.map(x=>(x.getString(2),x))
         actionRDD.mapPartitions(x=>{
           val listBuffer = new ListBuffer[(String, Row)]()
           while(x.hasNext)
             {
               val row=x.next()
               listBuffer.append((row.getString(2),row))
             }
           listBuffer.iterator
         })

  }

  /**
    * 对用户行为数据按session维度进行聚合
    * */
  def aggregateBySession(sc:SparkContext,sQLContext: SQLContext,session2actionRdd:RDD[(String,Row)]):RDD[(String,String)]={
    val session2ActionsRDD=session2actionRdd.groupByKey()

    /**
      * 得到聚合信息函数
      * */
    val getPartAggrInfo=(x:(String,Iterable[Row]))=>{

      var searchkeyBuffer=new StringBuffer()
      var clickCategoryIdsBuffer=new StringBuffer()
      var userId:Long=0

      var startTime:Date=null //开始时间
      var endTime:Date=null    //结束时间
      var stepLength:Int=0

        val iterator=x._2.iterator
      while(iterator.hasNext){
        val row=iterator.next()

           userId=row.getLong(1)

        val searchKey = row.getString(5)
        val clickCategoryId = row.getLong(6)

        val actionTime:Date=DateUtils.parseTime(row.getString(4))//获取动作时间

        if(StringUtils.isNotEmpty(searchKey))
          {
            if(!searchkeyBuffer.toString.contains(searchKey))
              searchkeyBuffer.append(searchKey+",")
          }
        if(clickCategoryId!=null)
          {
            if(!clickCategoryIdsBuffer.toString.contains(String.valueOf(clickCategoryId)))
              clickCategoryIdsBuffer.append(clickCategoryId+",")
          }

        if(startTime==null) startTime=actionTime
        if(endTime==null) endTime=actionTime
        if(actionTime.before(startTime)) startTime=actionTime
        if(actionTime.after(endTime)) endTime=actionTime

        stepLength+=1
      }
      val searchKeywords: String = StringUtils.trimComma(searchkeyBuffer.toString)
      val clickCategoryIds: String = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      val visitLength:Long=(endTime.getTime()-startTime.getTime())/1000

      val partAggrInfo=Constants.FIELD_SESSION_ID+"="+x._1+"|"+
      Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords+"|"+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds+"|"+
      Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"+Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"+
      Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime)
      (userId,partAggrInfo)
    }


    val userid2PartAggrInfoRDD=session2ActionsRDD.map(x=>getPartAggrInfo(x))

    val sql="select * from user_info"
    val userinfoRDD=sQLContext.sql(sql).rdd

    val userid2infoRDD=userinfoRDD.map(x=>(x.getLong(0),x))

    //将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD=userid2PartAggrInfoRDD.join(userid2infoRDD)
    val getFullInfoRDD=(x:(Long,(String,Row)))=>{
      val partAggrInfo:String=x._2._1
      val userInfoRow=x._2._2

      val sessionid:String=StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)

      val age=userInfoRow.getInt(3)
      val professional=userInfoRow.getString(4)
      val city=userInfoRow.getString(5)
      val sex=userInfoRow.getString(6)
      val fullAggrInfo: String = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city +
        "|" + Constants.FIELD_SEX + "=" + sex
      (sessionid,fullAggrInfo)
    }

    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map(x => (getFullInfoRDD(x)))


    //数据倾斜解决方案之将reduce join 转化为 map join
    /*
    val userid2infoRDDlist=userid2infoRDD.collect()
    val userInfoBroadcast=sc.broadcast(userid2infoRDDlist)
    sessionid2FullAggrInfoRDD=userid2PartAggrInfoRDD.map(x=>{
        val userInfos=userInfoBroadcast.value
        val map=new mutable.HashMap[Long,Row]()
        for(userinfo<-userInfos)
          {
            map.put(userinfo._1,userinfo._2)
          }
        val userid=x._1
        val userInfoRow=map(userid)
        val partAggrInfo=x._2

      val sessionid:String=StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)

      val age=userInfoRow.getInt(3)
      val professional=userInfoRow.getString(4)
      val city=userInfoRow.getString(5)
      val sex=userInfoRow.getString(6)
      val fullAggrInfo: String = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city +
        "|" + Constants.FIELD_SEX + "=" + sex
      (sessionid,fullAggrInfo)
    })
   */

    //数据倾斜解决方案之sample采样倾斜key进行两次join
    /*
    val sampleRDD=userid2PartAggrInfoRDD.sample(false,0.1,9)
    val computedSampleRDD=sampleRDD.map(x=>(x._1,1)).reduceByKey(_+_)
    val skewedUserid=computedSampleRDD.map(x=>(x._2,x._1)).sortByKey(false).take(1)(0)._2  //获取数据倾斜的key
    val skewedRDD=userid2PartAggrInfoRDD.filter(x=>(x._1.equals(skewedUserid)))      //拆分RDD 获取倾斜的RDD
    val commonRDD=userid2PartAggrInfoRDD.filter(x=>(!x._1.equals(skewedUserid)))    //获取正常的RDD
    val skewedUserid2infoRDD=userid2infoRDD.filter(_._1.equals(skewedUserid)).flatMap(x=>{
      val random=new Random()
      val listBuffer=new ListBuffer[(String,Row)]()
      for(i<-0 to 100)
        {
          val prefix=random.nextInt(100)
          listBuffer.append((prefix+"_"+x._1,x._2))
        }
      listBuffer
    })
   val joinedRDD1=skewedRDD.map(x=>{
     val random=new Random()
     val prefix=random.nextInt(100)
     (prefix+"_"+x._1,x._2)
   }).join(skewedUserid2infoRDD).map(x=>{
     val userid=x._1.split("_")(1).toLong
     (userid,x._2)
   })
    val joinedRDD2=commonRDD.join(userid2infoRDD)
    val joinedRDD=joinedRDD1.union(joinedRDD2)
    val sessionid2FullAggrInfoRDD=joinedRDD.map(x=>{
      val partAggrInfo=x._2._1
      val userInfoRow=x._2._2
      val sessionid:String=StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)

      val age=userInfoRow.getInt(3)
      val professional=userInfoRow.getString(4)
      val city=userInfoRow.getString(5)
      val sex=userInfoRow.getString(6)
      val fullAggrInfo: String = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city +
        "|" + Constants.FIELD_SEX + "=" + sex
      (sessionid,fullAggrInfo)
    })
   */
     sessionid2FullAggrInfoRDD

  }

  /**
    * 过滤session数据
    * */
  def filterSession(session2FullAggrInfo:RDD[(String,String)],taskParam:JSONObject,
                    sessionAggrStatAccumulator: Accumulator[String]):RDD[(String,String)]={

    val startAge=ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge=ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var parameter: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
    else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|"
    else "") + (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|"
    else "") + (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|"
    else "") + (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|"
    else "") + (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|"
    else "") + (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds
    else "")

    if(parameter.endsWith("\\|"))
      {
        parameter=parameter.substring(0,parameter.length-1)
      }


    /**
      * 计算访问时长范围
      * */
    def caculateVisitLength(visitLength:Long)={
      if (visitLength >= 1 && visitLength <= 3) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
      else if (visitLength >= 4 && visitLength <= 6) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
      else if (visitLength >= 7 && visitLength <= 9) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
      else if (visitLength >= 10 && visitLength <= 30) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
      else if (visitLength > 30 && visitLength <= 60) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
      else if (visitLength > 60 && visitLength <= 180) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
      else if (visitLength > 180 && visitLength <= 600) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
      else if (visitLength > 600 && visitLength <= 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
      else if (visitLength > 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
    }
    /**
      * 计算访问步长范围
      * */
    def caculateStepLength(stepLength:Long)={
      if (stepLength >= 1 && stepLength <= 3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
      else if (stepLength >= 4 && stepLength <= 6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
      else if (stepLength >= 7 && stepLength <= 9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
      else if (stepLength >= 10 && stepLength <= 30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
      else if (stepLength > 30 && stepLength <= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
      else if (stepLength > 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
    }


    //过滤RDD的函数
    def filtersession(x:(String,String)):Boolean={
      val aggrInfo=x._2
      if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE))
        return false
        if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS))
          return false
      if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES))
        return false
      if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX))
        return false
      if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_KEYWORDS))
        return false
      if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGORY_IDS))
        return false


        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

        val visitLength: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

        caculateVisitLength(visitLength)
        caculateStepLength(stepLength)

      return true
    }



     val  filterSessionRDD=session2FullAggrInfo.filter(x=>filtersession(x))

    filterSessionRDD
  }

/**
  * 计算存储统计分析结果
  * */
  def calculateAndPersistAggrStat(value:String,taskId:Long): Unit =
  {
    val session_count: Long =(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT)).toLong

    val visit_length_1s_3s: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s)).toLong
    val visit_length_4s_6s: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s)).toLong
    val visit_length_7s_9s: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s)).toLong
    val visit_length_10s_30s: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s)).toLong
    val visit_length_30s_60s: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s)).toLong
    val visit_length_1m_3m: Long =(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m)).toLong
    val visit_length_3m_10m: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m)).toLong
    val visit_length_10m_30m: Long =(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m)).toLong
    val visit_length_30m: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m)).toLong

    val step_length_1_3: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3)).toLong
    val step_length_4_6: Long =(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6)).toLong
    val step_length_7_9: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9)).toLong
    val step_length_10_30: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30)).toLong
    val step_length_30_60: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60)).toLong
    val step_length_60: Long = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60)).toLong

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)

    val step_length_1_3_ratio: Double = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio: Double = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio: Double = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio: Double = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio: Double = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio: Double = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat: SessionAggrStat = new SessionAggrStat()
    sessionAggrStat.setTaskid(taskId)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    val isessionAggrStatDAO=DAOFactory.getISessionAggrStatDao
    isessionAggrStatDAO.insert(sessionAggrStat)
  }


  /**
    * 随机抽取session
    * */
  def randomExtractSession(sc: SparkContext,taskid:Long,sessionid2AggrInfoRDD:RDD[(String,String)],sessionid2ActionRDD:RDD[(String,Row)]): Unit ={
    //1.计算每天每小时的session数量 (yyyy-MM-dd_HH,aggrinfo)
    val time2sessionidRDD=sessionid2AggrInfoRDD.map(x=>{
      val aggrInfo=x._2
      val startTime=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME)
      val dateHour=DateUtils.getDateHour(startTime)
      (dateHour,aggrInfo)
    })
    //得到每天每小时的session数量
    val countMap=time2sessionidRDD.countByKey()

    //2.将(yyyy-MM-dd_HH,count)的map，转换成（yyyy-MM-dd,(HH,count)）的格式

    val dateHourCountMap=new mutable.HashMap[String,mutable.HashMap[String,Long]]()

    for(countEntry<-countMap)
      {
        val dataHour=countEntry._1
        val date=dataHour.split("_")(0)
        val hour=dataHour.split("_")(1)

        val count:Int=countEntry._2.toInt

      val hourCountMap=dateHourCountMap.getOrElse(date,new mutable.HashMap[String,Long]())
        dateHourCountMap.put(date,hourCountMap)
        hourCountMap.put(hour,count)
      }
      //随机抽取算法实现
    //总共要抽取100个session，先按照天数进行平分
     val extractNumberPerDay=100/dateHourCountMap.size

    /**
      * session随机抽取功能
      *
      * 用到了一个比较大的变量，随机抽取索引map
      * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
      * 还是比较消耗内存和网络传输性能的
      *
      * 将map做成广播变量
      *
      */
     val dateHourExtractMap=new mutable.HashMap[String,mutable.HashMap[String,mutable.ListBuffer[Long]]]()

     val random=new Random()

    for(dateHourCountEntry<-dateHourCountMap)
      {
        val date=dateHourCountEntry._1
        val hourCountMap=dateHourCountEntry._2
        //计算这一天的session总数
        var sessionCount:Long=0
        for(hourCount<-hourCountMap.values)
          {
            sessionCount+=hourCount
          }
        val hourExtractMap = dateHourExtractMap.getOrElse(date, new mutable.HashMap[String, mutable.ListBuffer[Long]]())

        dateHourExtractMap.put(date,hourExtractMap)



        //遍历每个小时
        for(hourCountEntry<-hourCountMap)
          {
            val hour:String=hourCountEntry._1
            val count:Long=hourCountEntry._2

            //计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
            //就可以计算出，当前小时需要抽取的session数量

            var hourExtractNumber = ((count.toDouble / sessionCount.toDouble) * extractNumberPerDay).toInt

            if(hourExtractNumber>count) hourExtractNumber=count.toInt

            //先获取当前存放随机数的list
            val extractIndexList = hourExtractMap.getOrElse(hour, new mutable.ListBuffer[Long]())

            hourExtractMap.put(hour,extractIndexList)

            //s生成上面计算出来的随机数
            for(i<-0 to hourExtractNumber)
              {
                var extractIndex=random.nextInt(count.toInt)
                while(extractIndexList.contains(extractIndex))
                  {
                    extractIndex=random.nextInt(count.toInt)
                  }
                extractIndexList.append(extractIndex)
              }
          }
      }

    //广播变量
     val dateHourExtractMapBroadcast=sc.broadcast(dateHourExtractMap)

    /**
      * 3.遍历每天每小时的session，然后根据随机索引进行抽取
      * */
      val time2sessionidsRDD=time2sessionidRDD.groupByKey()



      /**
        * 返回抽取的session的rdd
        * */

      val extractSessionidsRDD:RDD[(String,String)]=time2sessionidsRDD.flatMap(x=>{
        val extractsessionids=new ListBuffer[(String,String)]
        val iterator=x._2.toIterator
        val dateHour=x._1
        val date=dateHour.split("_")(0)
        val Hour=dateHour.split("_")(1)

        /**
          * 使用广播变量的时候
          * 直接调用广播变量（Broadcast类型）的value() / getValue()
          * 可以获取到之前封装的广播变量
          */
        val extractList=dateHourExtractMapBroadcast.value(date)(Hour)

        var index:Int=0
        while(iterator.hasNext)
          {
            val sessionAggrInfo=iterator.next()

            if(extractList.contains(index))  {
              val sessionid=StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID)

              // 将数据写入MySQL
              val sessionRandomExtract: SessionRandomExtract = new SessionRandomExtract
              sessionRandomExtract.setTaskid(taskid)
              sessionRandomExtract.setSessionid(sessionid)
              sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME))
              sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
              sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))

              val iSessionRandomExtractDAO:ISessionRandomExtractDAO =DAOFactory.getSessionRandomExtractDAO
              iSessionRandomExtractDAO.insert(sessionRandomExtract)


              extractsessionids.append((sessionid,sessionid))
            }
            index+=1
          }

        extractsessionids
      })

    /**
      * 获取抽取出来的session的明细数据
      * */

    val extractSessionDetailRDD=extractSessionidsRDD.join(sessionid2ActionRDD)

    //插入数据库
   /*
    extractSessionDetailRDD.foreach(x=>{
         val row=x._2._2
          val sessionDetail = new SessionDetail()
      				sessionDetail.setTaskid(taskid)
      				sessionDetail.setUserid(row.getLong(1))
      				sessionDetail.setSessionid(row.getString(2))
      				sessionDetail.setPageid(row.getLong(3))
      				sessionDetail.setActionTime(row.getString(4))
      				sessionDetail.setSearchKeyword(row.getString(5))
              sessionDetail.setClickCategoryId(row.getLong(6))
      				sessionDetail.setClickProductId(row.getLong(7))
      				sessionDetail.setOrderCategoryIds(row.getString(8))
      				sessionDetail.setOrderProductIds(row.getString(9))
      				sessionDetail.setPayCategoryIds(row.getString(10))
      				sessionDetail.setPayProductIds(row.getString(11))

      				val sessionDetailDAO = DAOFactory.getSessionDetailDAO()
      				sessionDetailDAO.insert(sessionDetail)
    })*/

    extractSessionDetailRDD.foreachPartition(iterator=>{
      val sessionDetails=new util.LinkedList[SessionDetail]()
      while(iterator.hasNext)
        {
          val tuple=iterator.next()
          val row =tuple._2._2
          val sessionDetail = new SessionDetail()
          sessionDetail.setTaskid(taskid)
          sessionDetail.setUserid(row.getLong(1))
          sessionDetail.setSessionid(row.getString(2))
          sessionDetail.setPageid(row.getLong(3))
          sessionDetail.setActionTime(row.getString(4))
          sessionDetail.setSearchKeyword(row.getString(5))
          sessionDetail.setClickCategoryId(row.getLong(6))
          sessionDetail.setClickProductId(row.getLong(7))
          sessionDetail.setOrderCategoryIds(row.getString(8))
          sessionDetail.setOrderProductIds(row.getString(9))
          sessionDetail.setPayCategoryIds(row.getString(10))
          sessionDetail.setPayProductIds(row.getString(11))
          sessionDetails.add(sessionDetail)
        }
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO()
      sessionDetailDAO.insertBatch(sessionDetails)
    })


  }


   /**
     * 获取top10热门品类
     * */
  def getTop10Category(sesssid2detailRDD:RDD[(String,Row)],taskid:Long):Array[(CategorySortKey,String)] ={
    /**
      * 1.获取session访问过的所有品类
      * */
    val categoryidRDD=sesssid2detailRDD.flatMap(x=>{
       var list=new ListBuffer[(Long,Long)]()

      val row=x._2

      val clickcategoryId=row.getLong(6)
      if(clickcategoryId!=null) list.append((clickcategoryId,clickcategoryId))
      val ordercategoryIds=row.getString(8)
      if(ordercategoryIds!=null)
        {
         for(ordercategoryId<-ordercategoryIds.split(","))
           {
             list.append((ordercategoryId.toLong,ordercategoryId.toLong))
           }
        }

      val paycategoryIds=row.getString(10)
      if(paycategoryIds!=null)
        {
          for(paycategoryId<-paycategoryIds.split(","))
            {
              list.append((paycategoryId.toLong,paycategoryId.toLong))
            }
        }
      list
    }).distinct()  //去重


        /**
          * 2.计算各品类的点击、下单和支付的次数
          * */
     //计算各品类的点击次数
      val clickCategoryId2CountRDD=sesssid2detailRDD.filter(x=>{x._2.get(6)!=null}).map(x=>(x._2.getLong(6),1)).reduceByKey(_+_)
    //数据倾斜解决方案之提高reduce端并行度
   //val clickCategoryId2CountRDD=sesssid2detailRDD.filter(x=>{x._2.get(6)!=null}).map(x=>(x._2.getLong(6),1)).reduceByKey(_+_,1000)
    //数据倾斜解决方案之使用随机key进行双重聚合
    /*
    val clickCategoryId2CountRDD=sesssid2detailRDD.filter(x=>{x._2.get(6)!=null}).map(x=>{
     val random=new Random()
     val prefix=random.nextInt(10)
     (prefix+"_"+x._2.getLong(6),1)
   }).reduceByKey(_+_).map(x=>{
     (x._1.split("_")(1),x._2)
   }).reduceByKey(_+_)
    */


    //计算各品类的下单次数
      val orderCategoryId2CountRDD=sesssid2detailRDD.filter(x=>x._2.getString(8)!=null).flatMap(x=>{
        val list=ListBuffer[(Long,Long)]()
        val row=x._2
        val categoryids=row.getString(8).split(",")
        for(categoryid<-categoryids)
          list.append((categoryid.toLong,1))
        list
      }).reduceByKey(_+_)
    //计算各品类的支付次数
    val payCategoryId2CountRDD=sesssid2detailRDD.filter(x=>x._2.getString(10)!=null).flatMap(x=>{
      val list =ListBuffer[(Long,Long)]()
      val row=x._2
      val paycategoryIds=row.getString(10).split(",")
      for(paycategoryId<-paycategoryIds)
        {
          list.append((paycategoryId.toLong,1))
        }
      list
    }).reduceByKey(_+_)


    /**
      * 3.join各品类与它的点击、下单和支付的次数
      * 最中返回的形式（categoryid,categoryid=?|clickcount=?|ordercount=?|paycount=?）
      * */

    val categoryid2countRDD_1=categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map(x=>{
      val categoryId=x._1
      val value=Constants.FIELD_CATEGORY_ID+"="+categoryId+"|"+Constants.FIELD_CLICK_COUNT+"="+x._2._2.getOrElse(0)
      (categoryId,value)
    })
    val categoryid2countRDD_2=categoryid2countRDD_1.leftOuterJoin(orderCategoryId2CountRDD).map(x=>{
      val categoryid=x._1
      val value=x._2._1+"|"+Constants.FIELD_ORDER_COUNT+"="+x._2._2.getOrElse(0)
      (categoryid,value)
    })
    val category2CountRDD=categoryid2countRDD_2.leftOuterJoin(payCategoryId2CountRDD).map(x=>{
      val categoryid=x._1
      val value=x._2._1+"|"+Constants.FIELD_PAY_COUNT+"="+x._2._2.getOrElse(0)
      (categoryid,value)
    })

    /**
      * 4.自定义实现排序key
      * */
    /**
      * 5.将数据映射成（CategorySortKey,info）格式的key,然后进行排序
      * */

    val sortKey2CountRDD=category2CountRDD.map(x=>{
      val value=x._2
      val clickcount=StringUtils.getFieldFromConcatString(value,"\\|",Constants.FIELD_CLICK_COUNT).toLong
      val ordercount=StringUtils.getFieldFromConcatString(value,"\\|",Constants.FIELD_ORDER_COUNT).toLong
      val paycount=StringUtils.getFieldFromConcatString(value,"\\|",Constants.FIELD_PAY_COUNT).toLong
       val categorySortKey=new CategorySortKey(clickcount ,ordercount ,paycount)
      (categorySortKey,value)
    })

    //倒序排列
    val sortedCategoryCountRDD=sortKey2CountRDD.sortByKey(false)



    /**
      * 用take（10）取出top10热门品类，并写入mysql
      * */

    val top10categoryList=sortedCategoryCountRDD.take(10)
    val top10categoryDAO=DAOFactory.getTop10CategoryDAO
    for(categorylist<-top10categoryList)
      {
        val countInfo=categorylist._2
        val categoryid=StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        val clickcount=StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
        val ordercount=StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
        val paycount=StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong

        val top10category=new Top10Category()
        top10category.setCategoryid(categoryid)
        top10category.setClickCount(clickcount)
        top10category.setOrderCount(ordercount)
        top10category.setPayCount(paycount)
        top10category.setTaskid(taskid)

        top10categoryDAO.insert(top10category)
      }
      top10categoryList
  }



  /**
    * 获取top10品类的top10session
    * */

  def getTop10Session(sc:SparkContext,sessionid2detailRDD:RDD[(String,Row)],taskid:Long,top10CategoryArray:Array[(CategorySortKey,String)]): Unit ={
    /**
      * 1.将top10Categoryid生成一份RDD
      * */
    var listBuffer=new ListBuffer[(Long,Long)]()
    for(top10category<-top10CategoryArray)
      {
        val categoryid=StringUtils.getFieldFromConcatString(top10category._2,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        listBuffer.append((categoryid,categoryid))
      }

    val top10categoryidRDD:RDD[(Long,Long)]=sc.parallelize(listBuffer)


    /**
      * 2.获取top10品类被各session点击的次数
      * */
    //将各session的deatil组合
    val sessionid2detailsRDD=sessionid2detailRDD.groupByKey()
    val categoryid2sessionidCountRDD=sessionid2detailsRDD.flatMap(x=>{
      var listBuffer=new ListBuffer[(Long,String)]()
      var CountMap=new mutable.HashMap[Long,Long]()
      val sessionid=x._1
      val rows=x._2
      val iterator=rows.iterator
      while(iterator.hasNext)
        {

          val row=iterator.next()
          if(row.get(6)!=null)
            {
              val clickcategoryid=row.getLong(6)
              var count:Long=CountMap.getOrElse(clickcategoryid,0)
              count+=1
              CountMap.put(clickcategoryid,count)
            }
        }

      //返回结果
      for(categoryCountMap<-CountMap){
          val clickcategoryid=categoryCountMap._1
          val count=categoryCountMap._2
          val value=sessionid+","+count
          listBuffer.append((clickcategoryid,value))
      }
      listBuffer
    })

    //获取到top10热门品类，被各个session点击的次数
    val top10CategorySessionCountRDD=top10categoryidRDD.join(categoryid2sessionidCountRDD).map(x=>(x._1,x._2._2))

    val top10CategprySessionCountsRDD=top10CategorySessionCountRDD.groupByKey()


    /**
      * 3.分组取TopN算法，获取每个品类的top10活跃用户
      * */
    //得到top10sessionRDD
    val top10SessionRDD=top10CategprySessionCountsRDD.flatMap(x=>{
      var listBuffer=new ListBuffer[(String,String)]()

      val categoryid=x._1
      val sessioncounts=x._2
      val top10Sessions=new Array[String](10)

      var loop=new Breaks

      val iterator=x._2.iterator
      while(iterator.hasNext)
        {
          val sessioncount=iterator.next()
          val sessionid=sessioncount.split(",")(0)
          val count=sessioncount.split(",")(1).toLong
         loop.breakable {
           for (i <- 0 to 10) {
             if (top10Sessions(i) == null) {
               top10Sessions(i) = sessioncount
               loop.break
             } else {
               val _count = top10Sessions(i).split(",")(1).toLong
               if (count > _count) {
                 for (j <- 9 to i) {
                   top10Sessions(j) = top10Sessions(j - 1)
                 }
                 top10Sessions(i) = sessioncount
                 loop.break
               }
             }
           }
         }
        }

      //将数据写入MySQL
      for(sessioncount<-top10Sessions)
        {
          if(sessioncount!=null)
            {
              val sessionid=sessioncount.split(",")(0)
              val count= sessioncount.split(",")(1).toLong

              // 将top10 session插入MySQL表
              val top10Session: Top10Session = new Top10Session
              top10Session.setTaskid(taskid)
              top10Session.setCategoryid(categoryid)
              top10Session.setSessionid(sessionid)
              top10Session.setClickCount(count)

              val top10SessionDAO: ITop10SessionDAO = DAOFactory.getTop10SessionDAO
              top10SessionDAO.insert(top10Session)

              listBuffer.append((sessionid,sessionid))
            }
        }
      listBuffer
    })

    /**
      * 4.获取top10活跃session的明细数据，并写入MySQL
      * */

    val sessionDetailRDD=top10SessionRDD.join(sessionid2detailRDD)
    sessionDetailRDD.foreach(x=>
    {
      val row=x._2._2
      val sessionDetail: SessionDetail = new SessionDetail
      sessionDetail.setTaskid(taskid)
      sessionDetail.setUserid(row.getLong(1))
      sessionDetail.setSessionid(row.getString(2))
      sessionDetail.setPageid(row.getLong(3))
      sessionDetail.setActionTime(row.getString(4))
      sessionDetail.setSearchKeyword(row.getString(5))
      sessionDetail.setClickCategoryId(row.getLong(6))
      sessionDetail.setClickProductId(row.getLong(7))
      sessionDetail.setOrderCategoryIds(row.getString(8))
      sessionDetail.setOrderProductIds(row.getString(9))
      sessionDetail.setPayCategoryIds(row.getString(10))
      sessionDetail.setPayProductIds(row.getString(11))

      val sessionDetailDAO: ISessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    })

  }






}

object UserVisitSessionAnalyzeSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName(Constants.SPARK_APP_NAME_SESSION).
      set("spark.storage.memoryFraction","0.5"). //降低cache的内存占比
      set("spark.shuffle.consolidateFiles","true").
      set("spark.shuffle.file.buffer","64k"). //map端缓存大小 默认32k
      set("spark.shuffle.memoryFraction","0.3"). //reduce端聚合堆内存占比 默认0.2
      set("spark.reducer.maxSizeInFlight","24").//reduce端缓存大小 默认48M
      set("spark.shuffle.io.maxRetries ","60"). //shuffle文件拉取失败的时候，最多重试次数  默认是3
      set("spark.shuffle.io.retryWait ","60").// 每一次拉取shuffle文件的时间间隔
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses(Array(classOf[CategorySortKey]))
    SparkUtils.setMaster(conf)
    /**
      * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
      * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
      * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
      * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
      */

    val sc: SparkContext = new SparkContext(conf)
    val userVisitSessionAnalyzeSpark = new UserVisitSessionAnalyzeSpark()


    val sQLContext: SQLContext = userVisitSessionAnalyzeSpark.getSQLContext(sc)

    userVisitSessionAnalyzeSpark.mockData(sc, sQLContext)

    val taskDAO: ITaskDAO = DAOFactory.getTaskDAO()

    val taskid: Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    val task = taskDAO.findById(taskid)

    if (task == null) {
      System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].")
      return
    }
    val taskParam:JSONObject = JSON.parseObject(task.getTaskParam)

    val actionRDD=userVisitSessionAnalyzeSpark.getActionRDDByDateRange(sQLContext,taskParam)
    val session2ActionRDD=userVisitSessionAnalyzeSpark.getSessionid2ActionRDD(actionRDD)

    /**
      * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
      *
      * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
      * StorageLevel.MEMORY_ONLY_SER()，第二选择
      * StorageLevel.MEMORY_AND_DISK()，第三选择
      * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
      * StorageLevel.DISK_ONLY()，第五选择
      *
      * 如果内存充足，要使用双副本高可靠机制
      * 选择后缀带_2的策略
      * StorageLevel.MEMORY_ONLY_2()
      *
      */
    session2ActionRDD.persist(StorageLevel.MEMORY_ONLY)
    val session2AggrInfoRDD=userVisitSessionAnalyzeSpark.aggregateBySession(sc,sQLContext,session2ActionRDD)

    println(session2AggrInfoRDD.count())
    for(action <- session2AggrInfoRDD.take(10))
      {
        println(action)
      }

    val sessionAggrStatAccumulator:Accumulator[String]=sc.accumulator("")(new SessionAggrStatAccumulator())

    val fiterSessionRDD=userVisitSessionAnalyzeSpark.filterSession(session2AggrInfoRDD,taskParam,sessionAggrStatAccumulator)

    println(fiterSessionRDD.count())
    for(action <- fiterSessionRDD.take(100))
    {
      println("filter:"+action)
    }

    //随机抽取session
    userVisitSessionAnalyzeSpark.randomExtractSession(sc,taskid,session2AggrInfoRDD,session2ActionRDD)

    //统计分析session聚合结果
    userVisitSessionAnalyzeSpark.calculateAndPersistAggrStat(sessionAggrStatAccumulator.value,taskid)
    //得到热门10个品类
   val top10Category= userVisitSessionAnalyzeSpark.getTop10Category(session2ActionRDD,taskid)

    //得到热门品类top10session

    userVisitSessionAnalyzeSpark.getTop10Session(sc,session2ActionRDD,taskid,top10Category)
    sc.stop()

  }

}
