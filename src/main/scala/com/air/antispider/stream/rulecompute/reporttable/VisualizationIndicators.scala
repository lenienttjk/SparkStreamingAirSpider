package com.air.antispider.stream.rulecompute.reporttable


import java.sql.Date
import java.util.UUID

import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


/*

  * 计算 可视化数据

  * 数据来源是streaming存储到sparksql中的数据

  *

  * @author chengxc

  */

case class Requests(requestMethod: String,
                    request: String,
                    remoteAddr: String,
                    httpUserAgent: String,
                    timeIso8601: String,
                    serverAddr: String,
                    criticalCookie: String,
                    highFrqIPGroup: String,
                    flightType: String,
                    behaviorType: String,
                    travelType: String,
                    flightDate: String,
                    depcity: String,
                    arrcity: String,
                    JSESSIONID: String,
                    USERID: String,
                    queryRequestDataStr: String,
                    bookRequestDataStr: String,
                    httpReferrer: String,
                    StageTag: String,
                    Spider: Int)


case class BlackList(remoteAddr: String,
                     FlowID: String,
                     Score: String,
                     StrategicID: String)


object VisualizationIndicators {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setMaster("local[*]").setAppName("visualization")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    val session = SparkSession.builder().config(conf)


    import sqlContext.implicits._

    /*

      * 读取原始数据，注册为表

      */

    //数据路径

    val defaultPathConfig = "offlineConfig.properties"

    val filePath = PropertiesUtil.getStringByKey("inputFilePath", defaultPathConfig)

    //读取kafka中的原始日志信息
    val request = sc.textFile("E:/test/part-00000")
      .map(

        _.split("#CS#")).map(

      p => Requests(p(1), p(2), p(3), p(4), p(5), p(6), "", p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), "", 0)).toDF()

    //注册为表request

    request.registerTempTable("request")

    //读取黑名单信息

    val SpiderIP = sc.textFile("E:/test/part-00000/SpiderIP.txt").map(_.split("\\|")).map(p => BlackList(p(0), p(1), p(2), p(3))).toDF()

    //注册为表SpiderIP

    SpiderIP.registerTempTable("SpiderIP")

  }
}

