package com.tongji.cims.spark.ad

import java.util
import java.util.Date

import cn.tongji.cmis.conf.ConfigurationManager
import cn.tongji.cmis.constant.Constants
import cn.tongji.cmis.dao.IAdClickTrendDAO
import cn.tongji.cmis.dao.factory.DAOFactory
import cn.tongji.cmis.domain._
import cn.tongji.cmis.util.{DateUtils, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by 626hp on 2017/11/9.
  */
object AdClickRealTimeStatSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdClickRealTimeStatSpark").setMaster("local[2]")
   // conf.set("spark.driver.allowMultipleContexts","true")
    //				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //				.set("spark.default.parallelism", "1000");
    //				.set("spark.streaming.blockInterval", "50");
    //				.set("spark.streaming.receiver.writeAheadLog.enable", "true");
   // SparkUtils.setMaster(conf)
  //  val sc=new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc=ssc.sparkContext
    ssc.checkpoint("hdfs://zhangchenlin:9000/streaming_checkpoint")
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val topicset = topics.split(",").toSet
    val brokers = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicset)
    //根据动态黑名单进行数据过滤
    val filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream)
   //生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
    // 最粗
    val adRealTimeStatDStream=calculateRealTimeStat(sc,filteredAdRealTimeLogDStream)

    calculateProviceTop3Ad(adRealTimeStatDStream)

    calculateAdClickCountByWindow(adRealTimeLogDStream)

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 根据黑名单进行过滤
    * */
  def filterByBlacklist(adRealTimeLogDStream: InputDStream[(String,String)]):DStream[(String,String)]={
    val filteredAdRealTimeLogDStream=adRealTimeLogDStream.transform(rdd=>{
      val adBlacklistDAO=DAOFactory.getAdBlacklistDAO()
      val adBlacklists=adBlacklistDAO.findAll()

      val tuples=new ListBuffer[(Long,Boolean)]
      import scala.collection.JavaConversions._  //如果不添加这个隐式转换  那么无法遍历java类
      for(adBlacklist<-adBlacklists) tuples.append((adBlacklist.getUserid,true))

      val sc=rdd.context
      val blacklistRDD=sc.parallelize(tuples)

      val mappedRDD=rdd.map(x=>(x._2.split(" ")(3).toLong,x))

      val joinedRDD=mappedRDD.leftOuterJoin(blacklistRDD)

      val filterRDD=joinedRDD.filter(x=>{
        val optioonal=x._2._2
       // if(optioonal.get) false else true
        optioonal match {
          case None => true
          case _=>false
        }
      })

      val resultRDD=filterRDD.map(x=>x._2._1)
      resultRDD
    })
    filteredAdRealTimeLogDStream
  }


  /**
      * 生成动态黑名单
      * */
    def generateDynamicBlacklist(filteredAdRealTimeLogDStream:DStream[(String,String)]): Unit = {
      //收集到的日志流：
      //timestamp,provice,city,userid,adid
      //时间点，省份，城市，用户id,广告id
      val dailyUserAdClickDstream=filteredAdRealTimeLogDStream.map(x=>{
        val log=x._2
        val logSplit=log.split(" ")
        val timestamp=logSplit(0)
        val date=new Date(timestamp.toLong)
        val dateKey=DateUtils.formatDate(date)
        val userid=logSplit(3)
        val adid=logSplit(4)

        val key=dateKey+"_"+userid+"_"+adid
        (key,1)
      }).reduceByKey(_+_)


      dailyUserAdClickDstream.foreachRDD(rdd=>{
        rdd.foreachPartition(records=>{
          val adUserClickCounts=new util.ArrayList[AdUserClickCount]()
          while(records.hasNext)
          {
            val tuple=records.next()

            val keySplit=tuple._1.split("_")
            val date=DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
            val userid=keySplit(1).toLong
            val adid=keySplit(2).toLong
            val clickCount=tuple._2
            val adUserClickCount=new AdUserClickCount()
            adUserClickCount.setDate(date)
            adUserClickCount.setUserid(userid)
            adUserClickCount.setAdid(adid)
            adUserClickCount.setClickCount(clickCount)
            adUserClickCounts.add(adUserClickCount)
          }
          val adUserClickCountDao=DAOFactory.getAdUserClickCountDAO
          adUserClickCountDao.updateBatch(adUserClickCounts)
        })
      })


      val blacklistDstream=dailyUserAdClickDstream.filter(x=>{
        val key=x._1
        val keySplit=key.split("_")
        val date=DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
        val userid=keySplit(1).toLong
        val adid=keySplit(2).toLong

        val adUserClickCountDao=DAOFactory.getAdUserClickCountDAO()
        val clickCount=adUserClickCountDao.findClickCountByMultiKey(date,userid,adid)

        if(clickCount>100) false else  true

      })
      val blacklistUseridDstream=blacklistDstream.map(x=>{
        val key=x._1
        val userid=key.split("_")(1).toLong
        userid
      })
      //去重
      val distinctBlacklistUseridDstream=blacklistUseridDstream.transform(rdd=>{
        rdd.distinct()
      })
      //存储黑名单
      distinctBlacklistUseridDstream.foreachRDD(rdd=>{
        rdd.foreachPartition(iterator=>{
          val adBlacklists=new util.ArrayList[AdBlacklist]()
          while(iterator.hasNext){
            val userid=iterator.next()
            val adBlacklist=new AdBlacklist()

            adBlacklist.setUserid(userid)
            adBlacklists.add(adBlacklist)
          }
          val adBlacklistDao=DAOFactory.getAdBlacklistDAO()
          adBlacklistDao.insertBatch(adBlacklists)
        })
      })
    }
/**
  * 计算广告流量实时统计
  * */

  def calculateRealTimeStat(sc:SparkContext,filteredAdRealTimeLogDStream:DStream[(String,String)]):DStream[(String,Int)]={
    val mappedDStream=filteredAdRealTimeLogDStream.map(x=>{
      val log=x._2
      val logSplit=log.split(" ")
      val timestamp=logSplit(0)
      val date=new Date(timestamp.toLong)
      val dateKey=DateUtils.formatDate(date)
      val provice=logSplit(1)
      val city=logSplit(2)
      val adid=logSplit(4)
      val key=dateKey+"_"+provice+"_"+city+"_"+adid
      (key,1)
    })

    val updateFunc=(iterator:Iterator[(String,Seq[Int],Option[Int])])=>{
      iterator.map{case(key,currcount,oldcount)=>(key,currcount.sum+oldcount.getOrElse(0))}
    }

    //累加点击数量
    val aggregatedDStream=mappedDStream.updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)

    //计算出来的最新结果同步到Mysql中

    aggregatedDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
           val adStats=new util.ArrayList[AdStat]()
           while(iter.hasNext){
             val tuple=iter.next()
             val keySplited=tuple._1.split("_")
             val date: String = keySplited(0)
             val province: String = keySplited(1)
             val city: String = keySplited(2)
             val adid: Long = keySplited(3).toLong
          //  val adid=1L
             val clickCount: Long = tuple._2

             val adStat: AdStat = new AdStat
             adStat.setDate(date)
             adStat.setProvince(province)
             adStat.setCity(city)
             adStat.setAdid(adid)
             adStat.setClickCount(clickCount)

             adStats.add(adStat)

           }
        val adStatDao=DAOFactory.getAdStatDAO
        adStatDao.updateBatch(adStats)

      })
    })
    aggregatedDStream
  }

/**
  * 计算每天各省份的top3热门广告
  * */
  def calculateProviceTop3Ad(adRealTimeStatDStream:DStream[(String,Int)]): Unit = {
    //adRealTimeStatDStream
    //每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量

    //rdd[yyyyMMdd_province_city_adid,clickcount]
    //[yyyyMMdd_provice_adid,clickcount]
    val rowsDStream = adRealTimeStatDStream.transform(rdd => {
      val dailyAdClickcountByProvinceRDD = rdd.map(x => {
        val keySplit = x._1.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)
        val clickCount = x._2
        val key = date + "_" + province + "_" + adid
        (key, clickCount)
      }).reduceByKey(_ + _)
      //将dailyAdClickcountByProvinceRDD转化为DataFrame
      //注册为一张临时表
      //使用SparkSQL,通过开窗函数，获取到各省份的top3热门广告
      val rowsRDD = dailyAdClickcountByProvinceRDD.map(tuple => {
        val keySplit = tuple._1.split("_")
        val datekey = keySplit(0)
        val provice = keySplit(1)
        val adid = keySplit(2).toLong
        val clickcount = tuple._2.toLong
        val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))
        Row(date, provice, adid, clickcount)
      })
      val Schema = StructType(List(
        StructField("date", StringType, true),
        StructField("province", StringType, true),
        StructField("ad_id", LongType, true),
        StructField("click_count", LongType, true))
      )

      val sqlContext = new HiveContext(rdd.context)
      val dailyAdClickcountByProvinceDF = sqlContext.createDataFrame(rowsRDD, Schema)
      dailyAdClickcountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov")
      val provinceTop3AdDF = sqlContext.sql(
        "SELECT date,province,ad_id,click_count from (" +
          "select date,province,ad_id,click_count," +
          "row_number() over(partition by province order by click_count DESC) rank " +
          "from tmp_daily_ad_click_count_by_prov) t where rank<=3"
      )
      provinceTop3AdDF.rdd
    })

    //rowsDStream 每次都是刷新出来的各个省份最热门的top3广告 将其中的数据批量更新到mysql中

    rowsDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val adProvinceTop3s = new util.ArrayList[AdProvinceTop3]()
        while (iter.hasNext) {
          val row = iter.next()
          val date= row.getString(0)
          val province= row.getString(1)
          val adid = row.getLong(2)
          val clickCount = row.getLong(3)

          val adProvinceTop3: AdProvinceTop3 = new AdProvinceTop3()
          adProvinceTop3.setDate(date)
          adProvinceTop3.setProvince(province)
          adProvinceTop3.setAdid(adid)
          adProvinceTop3.setClickCount(clickCount)

          adProvinceTop3s.add(adProvinceTop3)
        }
        val adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
        adProvinceTop3DAO.updateBatch(adProvinceTop3s)
      })
    })
  }
   /**
     * 计算最近一个小时滑动窗口内的广告点击趋势
     * */
    def calculateAdClickCountByWindow(adRealTimeLogDStream:InputDStream[(String,String)]): Unit ={
         //映射成[yyyyMMddHHMM_adid,1]
       val pairDStream =adRealTimeLogDStream.map(x=>{
         val log=x._2
         val keySplit=log.split(" ")
         val time=DateUtils.formatTimeMinute(new Date(keySplit(0).toLong))
         val adid=keySplit(4)
         (time+"_"+adid,1)
       })
     val aggrRDD=pairDStream.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(3600),Seconds(10))

      aggrRDD.foreachRDD(rdd=>{
        rdd.foreachPartition(iter=>{
          val adClickTrends: util.List[AdClickTrend] = new util.ArrayList[AdClickTrend]

          while (iter.hasNext) {
            val tuple = iter.next
            val keySplited: Array[String] = tuple._1.split("_")
            // yyyyMMddHHmm
            val dateMinute: String = keySplited(0)
            val adid: Long =keySplited(1).toLong
            val clickCount: Long = tuple._2
            val date: String = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
            val hour: String = dateMinute.substring(8, 10)
            val minute: String = dateMinute.substring(10)
            val adClickTrend: AdClickTrend = new AdClickTrend
            adClickTrend.setDate(date)
            adClickTrend.setHour(hour)
            adClickTrend.setMinute(minute)
            adClickTrend.setAdid(adid)
            adClickTrend.setClickCount(clickCount)
            adClickTrends.add(adClickTrend)
          }

          val adClickTrendDAO: IAdClickTrendDAO = DAOFactory.getAdClickTrendDAO
          adClickTrendDAO.updateBatch(adClickTrends)
        })
      })
    }







}
