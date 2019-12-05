package com.air.antispider.stream.rulecompute.luanch

import com.air.antispider.stream.common.util.hdfs.BlackListToRedis
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.rulecompute.businessprocess.{AnalyzeRuleDB, AntiCalculateResult, BroadcastProcess, CoreRule, FlowScoreResult, RuleUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ArrayBuffer

/**
  * 实时统计主类
  */
object ComputeLaunch {

  def main(args: Array[String]): Unit = {

    // 当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    // 初始化Spark
    val conf = new SparkConf().setAppName("StreamingRealTimeCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 创建SQL环境
    val sQLContext = new SQLContext(sc)
    // 配置Kafka信息
    // 设置消费者组
    val groupId = "g2"
    // kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" ->
        PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 设置从头消费
      "auto.offset.reset" -> "earliest"
    )
    // topic
    val topics = Set(PropertiesUtil.getStringByKey("target.query.topic", "kafkaConfig.properties"))
    // 创建流处理方法
    val ssc = setupSsc(sc, sQLContext, kafkaParams, topics)

    // 启动
    ssc.start()
    ssc.awaitTermination()

  }


  def setupSsc(sc: SparkContext,
               sqlContext: SQLContext,
               kafkaParams: Map[String, Object],
               topics: Set[String]): StreamingContext = {


    // 创建streaming
    val ssc = new StreamingContext(sc, Seconds(3))


    // 消费kafka数据
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )


    // todo 获取查询关键页面规则
    val queryCriticalPages = AnalyzeRuleDB.queryCriticalPages()
    @volatile var broadcastQueryCriticalPages = sc.broadcast(queryCriticalPages)

    //todo  mysql 中的 黑名单规则
    val ipInitList = AnalyzeRuleDB.queryIpBlackList()
    @volatile var broadcastIPList = sc.broadcast(ipInitList)


    //todo 获取流程规则策略配置规则
    // createFlow(0) 为反爬虫流程  createFlow(1) 为防占座流程 createFlow(0) 调用了 createRuleList（）
    val flowList = AnalyzeRuleDB.createFlow(0)
    @volatile var broadcastFlowList = sc.broadcast(flowList)


    // 数据处理,得到strem数据
    val lines = stream.map(_.value())
    //  将数据封装
    val structuredDataLines = QueryDataPackage.queryDataLoadAndPackage(lines)

    // 开启redis的常连接
    val jedis = JedisConnectionUtil.getJedisCluster

    lines.foreachRDD(rdd => {

      // 更新关键页面广播变量
      broadcastQueryCriticalPages = BroadcastProcess.monitorQueryCriticalPagesRule(sc, broadcastQueryCriticalPages, jedis)

      // 更新黑名单广播变量
      broadcastIPList = BroadcastProcess.monitorIpBlackListRule(sc, broadcastIPList, jedis)

      // 更新流程规则策略配置广播变量
      broadcastFlowList = BroadcastProcess.monitorFlowListRule(sc, broadcastFlowList, jedis)

    })


    // TODO   指标1：单位时间内 的IP段访问量（前两位）
    val ipCountsMap = CoreRule.ipBlockAccessCounts(structuredDataLines, Seconds(9), Seconds(3))
    // 转化成Map()
    var ipBlockAccessCountsMap = scala.collection.Map[String, Int]()
    ipCountsMap.foreachRDD(rdd => {
      ipBlockAccessCountsMap = rdd.collectAsMap()
    })

    // TODO   指标2：单位时间内 IP总访问量
    val ipAccessCounts = CoreRule.ipAccessCounts(structuredDataLines, Seconds(9), Seconds(3))
    // 转化成Map()
    var ipAccessCountsMap = scala.collection.Map[String, Int]()
    ipAccessCounts.foreachRDD(rdd => {
      ipAccessCountsMap = rdd.collectAsMap()
    })


    // TODO   指标3：关键页面访问量
    val criticalPagesCounts = CoreRule.criticalPagesCounts(structuredDataLines, Seconds(9), Seconds(3), broadcastQueryCriticalPages.value)
    // 转化成Map()
    var criticalPagesCountsMap = scala.collection.Map[String, Int]()
    criticalPagesCounts.foreachRDD(rdd => {
      criticalPagesCountsMap = rdd.collectAsMap()
    })


    // TODO 指标4: 某个IP，单位时间内的UA 种类数量统计
    val userAgentCounts = CoreRule.userAgent(structuredDataLines, Seconds(9), Seconds(3))
    var userAgentCountsMap = scala.collection.Map[String, Int]()
    userAgentCounts.map(t => {
      val ua = t._2
      val counts = RuleUtil.diffUserAgent(ua)
      (t._1, counts)
    }).foreachRDD(rdd => {
      userAgentCountsMap = rdd.collectAsMap()
    })


    // TODO 指标5:  某个IP，单位时间内的关键页面最短访问间隔(最小时间差)
    val criticalMinInterval = CoreRule.criticalPagesAccTime(
    structuredDataLines, Seconds(9), Seconds(3), broadcastQueryCriticalPages.value)
    // 转化成Map()
    var criticalMinIntervalMap = scala.collection.Map[String, Int]()
    criticalMinInterval.map(t => {
      val accTimes = t._2

      //  计算 时间差
      val list = RuleUtil.calculateIntervals(accTimes)

       // 计算 得到最小时间间隔
      (t._1, RuleUtil.minInterval(list))

    }).foreachRDD(rdd => {
      criticalMinIntervalMap = rdd.collectAsMap()
    })





    // TODO 指标6: 某个IP，单位时间内小于最短访问间隔（自设）的关键页面查询次数
    val aCriticalPagesAccTime = CoreRule.aCriticalPagesAccTime(
      structuredDataLines, Seconds(9), Seconds(3), broadcastQueryCriticalPages.value)
    // 转化成Map()
    var aCriticalPagesAccTimeMap = scala.collection.Map[(String, String), Int]()

    aCriticalPagesAccTime.map(t => {
      val accTime = t._2
      (t._1, RuleUtil.calculateMinTimes(accTime))
    }).foreachRDD(rdd => {
      aCriticalPagesAccTimeMap = rdd.collectAsMap()
    })





    // TODO 指标7: 某个IP，5分钟内 不同行程的 查询次数

    val flightQuery = CoreRule.flightQuerys(structuredDataLines, Seconds(9), Seconds(3))
    var differentTripQuerysMap = scala.collection.Map[String, Int]()
    flightQuery.map(rdd => {

      val query = rdd._2
      val count = RuleUtil.calculateDifferentTripQuerys(query)

      (rdd._1, count)
    }).foreachRDD(rdd => {
      differentTripQuerysMap = rdd.collectAsMap()
    })





    // TODO 指标8:  某个IP，关键页面访问Cookie数
    val criticalCookie = CoreRule.criticalCookies(
      structuredDataLines, Seconds(9), Seconds(3), broadcastQueryCriticalPages.value)
    var criticalCookiesMap = scala.collection.Map[String, Int]()
    criticalCookie.map(rdd => {
      val cookie = rdd._2
      (rdd._1, RuleUtil.cookiesCounts(cookie))
    }).foreachRDD(rdd => {
      criticalCookiesMap = rdd.collectAsMap()
    })






     //  TODO 流程打分
    //  TODO 反爬虫指标结果  根据各流程进行规则匹配和打分并进行阈值判断
    val antiCalculateResults = structuredDataLines.map { processedData =>
      //获取ip和request，从而可以从上面的计算结果Map中得到这条记录对应的5分钟内总量
      // 从而匹配数据库流程规则
      val ip = processedData.remoteAddr
      val request = processedData.request

      // 封装到 反爬虫结果
      val antiCalculateResult = RuleUtil.calculateAntiResult(

        processedData,
        broadcastFlowList.value.toArray,
        ip,
        request,

        ipBlockAccessCountsMap,
        ipAccessCountsMap,
        criticalPagesCountsMap,
        userAgentCountsMap,

        criticalMinIntervalMap,
        aCriticalPagesAccTimeMap,
        differentTripQuerysMap,
        criticalCookiesMap)
      antiCalculateResult
    }


    //todo  剔除非黑名单数据(各流程打分结果均小于阈值,
    // 我们值设置了一个流程，所以只会循环一次
    val antiBlackResults = antiCalculateResults.filter { antiCalculateResult =>

      val upLimitedFlows = antiCalculateResult.flowsScore.filter { flowScore =>

        //阈值判断结果，打分值大于阈值，为true
        flowScore.isUpLimited
      }
      //数据非空，说明存在大于阈值的流程打分
      upLimitedFlows.nonEmpty
    }



    // todo  redis黑名单去重、恢复
    antiBlackResults.map { antiBlackResult =>
      //黑名单ip，黑名单打分
      (antiBlackResult.ip, antiBlackResult.flowsScore)
    }.foreachRDD { rdd =>
      //过滤掉重复的数据，（ip，流程分数）
      val distincted: RDD[(String, Array[FlowScoreResult])] = rdd.reduceByKey((x, y) => x)
      //反爬虫黑名单数据（ip，流程分数）
      val antiBlackList: Array[(String, Array[FlowScoreResult])] = distincted.collect()
      if (antiBlackList.nonEmpty) {
        //黑名单DataFrame-备份到HDFS
        val antiBlackListDFR = new ArrayBuffer[Row]
        try {
          //创建jedis连接
          val jedis = JedisConnectionUtil.getJedisCluster
          // 恢复redis黑名单数据，用于防止程序停止而产生的redis数据丢失
          BlackListToRedis.blackListDataToRedis(jedis, sc, sqlContext)
        }
      }
    }







    // 返回
    ssc
  }
}

