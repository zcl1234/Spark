package com.tongji.cims.spark.product

import java.util

import cn.tongji.cmis.conf.ConfigurationManager
import cn.tongji.cmis.constant.Constants
import cn.tongji.cmis.dao.factory.DAOFactory
import cn.tongji.cmis.domain.AreaTop3Product
import cn.tongji.cmis.util.{ParamUtils, SparkUtils}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by 626hp on 2017/11/7.
  * 获取各区域热门Top3商品
  */
object AreaTop3ProductSpark {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("AreaTop3ProductSpark")
    conf.set("spark.testing.memory", "2147480000")
    SparkUtils.setMaster(conf)
    val sc =new SparkContext(conf)

    val sQLContext=SparkUtils.getSQLContext(sc)
    //注册UDF函数
    sQLContext.udf.register("concat_long_string",(a:Long,b:String)=>(a.toString+":"+b))
    sQLContext.udf.register("get_json_object",(json:String,field:String)=>{
     val jsonobject=JSON.parseObject(json)
      jsonobject.getString(field)
    })

   //注册UDAF函数
    sQLContext.udf.register("group_concat_distinct",new GroupContactDistinctUDAF)
    SparkUtils.mockData(sc,sQLContext)

    val taskDAO=DAOFactory.getTaskDAO
    val taskid=ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT)
    val task=taskDAO.findById(taskid)

    val taskParam=JSON.parseObject(task.getTaskParam)
    val startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val cityid2ActionRDD=getcityid2ClickActionRDDByDate(sQLContext,startDate,endDate)
    val cityid2InfoRDD=getcityid2infoRDD(sQLContext)


    generateTempClickProductBasicTable(sQLContext,cityid2ActionRDD,cityid2InfoRDD)
    generateTempAreaProductClickCountTable(sQLContext)
    generateTempAreaFullProductClickCountTable(sQLContext)
    val rdd=getAreaTop3ProductRDD(sQLContext)
    persistAreaTop3Product(taskid,rdd.collect())

  }

  /**
    * 获取指定日期内的用户点击行为
    * */
  def getcityid2ClickActionRDDByDate(sQLContext: SQLContext,startDate:String,endDate:String):RDD[(Long,Row)]={
    val sql: String = "SELECT " + "city_id," +
      "click_product_id product_id " + "FROM user_visit_action " +
      "WHERE click_product_id IS NOT NULL " +
      "AND date>='" + startDate + "' " + "AND date<='" + endDate + "'"
    val clickActionDF=sQLContext.sql(sql)
    val clickActionRDD=clickActionDF.rdd
    val cityid2ActionRDD=clickActionRDD.map(x=>{(x.getLong(0),x)})
    cityid2ActionRDD
  }
 /**
   * SQLContext从mysql中获取城市信息
   * */
  def getcityid2infoRDD(sQLContext: SQLContext):RDD[(Long,Row)]={
    var url=""
    var user=""
    var password=""
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local)
      {
        url=ConfigurationManager.getProperty(Constants.JDBC_URL)
        user = ConfigurationManager.getProperty(Constants.JDBC_USER)
        password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
      }else{
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
    }

    val options=new mutable.HashMap[String,String]()

    options.put("url", url)
    options.put("dbtable", "city_info")
    options.put("user", user)
    options.put("password", password)

    val cityInfoDF=sQLContext.read.format("jdbc").options(options).load()
    val cityInfoRDD=cityInfoDF.rdd
    val cityid2InfoRDD=cityInfoRDD.map(x=>(x.getLong(0),x))

    cityid2InfoRDD
  }

  /**
    * 生成点击商品基础信息临时表
    * */
  def generateTempClickProductBasicTable(sQLContext: SQLContext,cityid2ActionRDD:RDD[(Long,Row)],cityid2InfoRDD:RDD[(Long,Row)]): Unit ={

    val joinedRDD=cityid2ActionRDD.join(cityid2InfoRDD)
    // 将上面的RDD，转换成一个RDD<Row>（才能将RDD转换为DataFrame）
    val mappedRDD=joinedRDD.map(tuple=>{
      val cityid=tuple._1
      val productid=tuple._2._1.getLong(1)
      val cityname=tuple._2._2.getString(1)
      val area=tuple._2._2.getString(2)
      Row(cityid,cityname,area,productid)
    })
   //基于RDD[Row]的格式 可以将其转换为DataFrame
    val schema=StructType(
      List(
        StructField("city_id",LongType,true),
        StructField("city_name",StringType,true),
        StructField("area",StringType,true),
        StructField("product_id",LongType,true)
      )
    )
    val productDataFrame=sQLContext.createDataFrame(mappedRDD,schema)
    println("tmp_click_product_basic"+productDataFrame.count())
    productDataFrame.registerTempTable("tmp_click_product_basic")
  }

     /**
       * 生成各区域各商品点击次数临时表
       * */
  def generateTempAreaProductClickCountTable(sQLContext: SQLContext): Unit ={
    val sql="select area," +
      "product_id,count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name)) city_infos "+
      "from tmp_click_product_basic" +
      " group by area,product_id"
    val dataframe=sQLContext.sql(sql)
    println("tmp_area_product_click_count:"+dataframe.count())
    dataframe.registerTempTable("tmp_area_product_click_count")
  }
  /**
    * 生成各区域各商品点击次数临时表（包含商品的完整信息）
    * */
    def generateTempAreaFullProductClickCountTable(sQLContext: SQLContext): Unit ={
      val sql="select "+
        "tapcc.area," +
        "tapcc.product_id,"+
        "tapcc.click_count," +
        "tapcc.city_infos," +
        "pi.product_name,"+
        "if(get_json_object(pi.extend_info,'product_status')='0','Self','Thrid Party') product_status"+
        " from tmp_area_product_click_count tapcc"+
        " JOIN product_info pi ON tapcc.product_id = pi.product_id"
      val dataframe=sQLContext.sql(sql)
      println("tmp_area_fullprod_click_count:"+dataframe.count())
      dataframe.registerTempTable("tmp_area_fullprod_click_count")
    }
  /**
    * 获取各区域top3热门商品
    * */
  def getAreaTop3ProductRDD(sQLContext: SQLContext): RDD[Row] ={

    val sql="select area," +
     "CASE " +
       "WHEN area='China North' OR area='China East' THEN 'A Level' "+
       "WHEN area='China South' OR area='China Middle' THEN 'B Level' "+
        "WHEN area='West North' OR area='West South' THEN 'C Level' "+
      "ELSE 'D Level' "+
      "END area_level,"+
      "product_id,click_count,city_infos,product_name,product_status "+
      "FROM (select area,product_id,click_count,city_infos,product_name,product_status,"+
    "row_number() OVER(partition by area order by click_count DESC) rank"+
    " from tmp_area_fullprod_click_count) t where rank<=3"

    //val  sql= "SELECT " + "area," + "CASE " + "WHEN area='China North' OR area='China East' THEN 'A Level' " + "WHEN area='China South' OR area='China Middle' THEN 'B Level' " + "WHEN area='West North' OR area='West South' THEN 'C Level' " + "ELSE 'D Level' " + "END area_level," + "product_id," + "click_count," + "city_infos," + "product_name," + "product_status " + "FROM (" + "SELECT " + "area," + "product_id," + "click_count," + "city_infos," + "product_name," + "product_status," + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " + "FROM tmp_area_fullprod_click_count " + ") t " + "WHERE rank<=3"
    val dataframe=sQLContext.sql(sql)

    dataframe.rdd

  }

  //持久化结果
  def persistAreaTop3Product(taskid:Long,rows:Array[Row]): Unit ={
     val areaTop3Products=new util.ArrayList[AreaTop3Product]()
    for(row<-rows)
      {
        val areaTop3Product=new AreaTop3Product()
        areaTop3Product.setTaskid(taskid)
        areaTop3Product.setArea(row.getString(0))
        areaTop3Product.setAreaLevel(row.getString(1))
        areaTop3Product.setProductid(row.getLong(2))
        areaTop3Product.setClickCount(row.get(3).toString.toLong)
        areaTop3Product.setCityInfos(row.getString(4))
        areaTop3Product.setProductName(row.getString(5))
        areaTop3Product.setProductStatus(row.getString(6))
        areaTop3Products.add(areaTop3Product)
      }
           val areatop3ProductDao=DAOFactory.getAreaTop3ProductDAO
    areatop3ProductDao.insertBatch(areaTop3Products)
  }


}
