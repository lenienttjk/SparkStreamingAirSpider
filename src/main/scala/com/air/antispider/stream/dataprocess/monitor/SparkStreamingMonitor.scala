package com.air.antispider.stream.dataprocess.monitor

import java.sql.Date
import java.text.SimpleDateFormat

import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

object SparkStreamingMonitor {
  def streamingMonitor(
                        sc: SparkContext,
                        rdd: RDD[String],
                        serversCountMap:collection.Map[String,Int],
                        jedis: JedisCluster): Unit = {

    val applicationId = sc.applicationId

    val appName = sc.appName

    val url = "http://localhost:4040/metrics/json/"

    val jSONObject = SparkMetricsUtils.getMetricsJson(url)

    val result = jSONObject.getJSONObject("gauges")



     // 监控信息的json路径： Application id+.driver.+应用名称+具体的监控指标名称


    //最近完成批次的处理"开始时间"-Unix时间戳（Unix timestamp）-单位：毫秒

    val startTimePath = applicationId + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"

    val startValue = result.getJSONObject(startTimePath)

    var processingStartTime: Long = 0

    if (startValue != null) {

      processingStartTime = startValue.getLong("value")

    }



    //最近完成批次的处理"结束时间"-Unix时间戳（Unix timestamp）-单位：毫秒

    //local-1517307598019.driver.streaming-data-peocess.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime":{"value":1517307625168}

    val endTimePath = applicationId + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"

    val endValue = result.getJSONObject(endTimePath)

    var processEndTime: Long = 0

    if (endValue != null) {

      processEndTime = endValue.getLong("value")

    }

    //最近批处理的数据行数
    val sourceCount = rdd.count()


    //最近批处理所花费的时间
    val costTime = processEndTime - processingStartTime


    //监控指标（计算 平均时间）:实时处理的速度监控指标-monitorIndex 需要写入Redis，
    // 由web端读取Redis并持久化到Mysql
    val countPerMillis = sourceCount.toFloat / costTime.toFloat


    //将数据封装到map中
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val processingEndTimeString = dataFormat.format(new Date(processEndTime))

    val fieldMap = scala.collection.mutable.Map(

      "endTime" -> processingEndTimeString,

      "applicationUniqueName" -> appName.toString,

      "applicationId" -> applicationId.toString,

      "sourceCount" -> sourceCount.toString,

      "costTime" -> costTime.toString,

      "countPerMillis" -> countPerMillis.toString,

      "serversCountMap" -> serversCountMap)



    /*

      *存储监控数据到redis

     */

    try {

//      val jedis = JedisConnectionUtil.getJedisCluster

      //监控记录有效期，单位秒
      val monitorDataExpTime: Int = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

      //产生不重复的key值
      val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + System.currentTimeMillis.toString
      val keyNameLast = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + "_LAST"


      //保存监控数据 到redis
      jedis.setex(keyName, monitorDataExpTime, Json(DefaultFormats).write(fieldMap))


      //更新最新的监控数据
      jedis.setex(keyNameLast, monitorDataExpTime, Json(DefaultFormats).write(fieldMap))

      // 清除数据
      //JedisConnectionUtil.returnRes(jedis)

    } catch {

      case e: Exception =>

        e.printStackTrace()

    }

  }

}
