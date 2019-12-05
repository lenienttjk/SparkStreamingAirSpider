package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean.{AnalyzeRule, ProcessedData}
import com.air.antispider.stream.common.util.decode.MD5
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess._
import com.air.antispider.stream.dataprocess.monitor.SparkStreamingMonitor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 运行数据处理的主类
  */
object DataProcessLauncher {

  def main(args: Array[String]): Unit = {
    // 当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    // 初始化Spark
    val conf = new SparkConf().setAppName("StreamingDataProcess").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 创建SQL环境
    val sQLContext = new SQLContext(sc)
    // 配置Kafka信息
    // 设置消费者组
    val groupId = "g1"
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
    val topics = Set(PropertiesUtil
      .getStringByKey("source.nginx.topic", "kafkaConfig.properties"))
    // 创建流处理方法
    val ssc = setupSsc(sc, sQLContext, kafkaParams, topics)

    // 启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 业务处理层
    *
    * @param sc SparkContext
    * @param sQLContext  sql
    * @param kafkaParams kafka参数
    * @param topics  主题
    * @return
    */
  def setupSsc(
                sc: SparkContext,
                sQLContext: SQLContext,
                kafkaParams: Map[String, Object],
                topics: Set[String]): StreamingContext = {

    // 创建streaming
    val ssc = new StreamingContext(sc, Seconds(3))
    // 消费kafka数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )


    // 读取数据库的过滤规则，然后同步到广播变量
    val filterRulelist = AnalyzeRuleDB.queryFilterRule()

    // 读取数据库分类规则，然后同步广播变量
    val ruleMapTemp = AnalyzeRuleDB.queryRuleMap()

    // 查询规则
    val queryRules = AnalyzeRuleDB.queryRule(0)

    // 预定规则
    val bookRules = AnalyzeRuleDB.queryRule(1)

    // 查询IP黑名单规则
    val ipBlackList = AnalyzeRuleDB.queryIpBlackList()


    //  Map 存储数据
    val queryOrBooks = new mutable.HashMap[String, List[AnalyzeRule]]()
    queryOrBooks.put("queryRules", queryRules)
    queryOrBooks.put("bookRules", bookRules)


    // 加 @volatile 是为了优化
    // 广播过滤规则
    // 广播分类规则
    // 广播查询预定规则
    // 广播ip黑名单
    @volatile var filterRuleRef = sc.broadcast(filterRulelist)
    @volatile var broadcastRuleMap = sc.broadcast(ruleMapTemp)
    @volatile var broadcastQueryBookRule = sc.broadcast(queryOrBooks)
    @volatile var ipBlackListRef = sc.broadcast(ipBlackList)






    // 开启redis的常连接
    val jedis = JedisConnectionUtil.getJedisCluster






    // 数据处理
    stream.map(_.value()).foreachRDD { rdd => {

      // 开启缓存
      rdd.cache()


      // 监控过滤规则是否发生改变
      filterRuleRef = BroadcastProcess.mointorBroadFilterRule(sc, filterRuleRef, jedis)

      // 监控分类规则是否发生改变
      broadcastRuleMap = BroadcastProcess.mointorClassiferRule(sc, broadcastRuleMap, jedis)

      // 监控解析规则是否发生变化
      broadcastQueryBookRule = BroadcastProcess.mointorQueryBooksRule(sc, broadcastQueryBookRule, jedis)

      // 监控数据库黑名单是否发生变化
      ipBlackListRef = BroadcastProcess.mointorBlackListRule(sc, ipBlackListRef, jedis)



      // 对数据处理，切分数据,形成accessLog 样例类结构数据
      val value = DataSplit.parseAccessLog(rdd)


      //  统计链路,linkCount增加返回值，供实时监控调用
      val serversCount  = BusinessProcess.linkCount(value, jedis)
       val serversCountMap = serversCount.collectAsMap()


      //  数据清洗功能
      val filterRdd = value.filter(log => UrlFilter.filterUrl(log, filterRuleRef))


      //   数据脱敏:  手机号  身份证   使用 MD5加密成字符串
      val dataProcess  = filterRdd.map(log => {

        // 手机号
        log.http_cookie = EncryedData.encryptedPhone(log.http_cookie)
        // 身份证
        log.http_cookie = EncryedData.encryptedId(log.http_cookie)





        //  分类打标签： 国内预定 国际预定 国内查询 国际查询
        val requestTypeLabel = RequestTypeClassifier.classifyByRequest(
          log.request, broadcastRuleMap.value)

        //  往返标签
        val travelTypeLabel = TravelTypeClassifier
          .classifyByRefererAndRequestBody(log)


        //  数据解析


        // 查询数据的解析

        // broadcastQueryBookRule.value.get("queryRules").get 等效于
        // val map = broadcastQueryBookRule.value
        // map("queryRules")

        // 查询数据的解析
        val queryBookMap: mutable.HashMap[String, List[AnalyzeRule]] = broadcastQueryBookRule.value

        val queryRequestData = AnalyzeRequest.analyzeQueryRequest(
          requestTypeLabel, log.request_method, log.content_type,
          log.request, log.request_body, travelTypeLabel,
          queryBookMap("queryRules"))

        // 预定数据的解析

        val bookRequestData = AnalyzeBookRequest.analyzeBookRequest(
          requestTypeLabel, log.request_method, log.content_type,
          log.request, log.request_body, travelTypeLabel,
          queryBookMap("bookRules"))


        //  判断高频Ip
        val highFrqIPGroup = IpOperation.isFreIP(log.remote_addr, ipBlackListRef.value)


        //  数据结构化
        DataPackage.dataPackage(
          "", log, highFrqIPGroup, requestTypeLabel, travelTypeLabel, queryRequestData, bookRequestData, log.http_referer)
      })


      /**
        * 数据推送到Kafka
        */
      // 查询行为数据推送到Kafka
      DataSend.sendQueryDataToKafka(dataProcess)

      // 预定行为数据推送到Kafka
      DataSend.sendBookDataToKafka(dataProcess)


      /**
        * 实时监控 模块
        *
        */
    SparkStreamingMonitor.streamingMonitor(sc,rdd,serversCountMap,jedis)


      // 释放资源
      rdd.unpersist()

    }}
    // 返回ssc
    ssc
  }
}
