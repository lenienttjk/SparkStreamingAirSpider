package com.air.antispider.stream.rulecompute.antispider

import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object QueryLauncher {

  def main(args: Array[String]): Unit = {
    // 当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    // 初始化Spark
    val conf = new SparkConf().setAppName("StreamingAirSpider").setMaster("local[*]")
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
    val topics = Set(PropertiesUtil.getStringByKey("target.query.topic", "kafkaConfig.properties"))

    // zk_servers
    val zkHosts = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")


    // 创建流处理方法
    val ssc = setupSsc(sc, sQLContext, kafkaParams,zkHosts, topics,groupId)

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
                zkHosts:String,
                topics: Set[String],
                groupId:String): StreamingContext = {

    // 创建streaming
    val ssc = new StreamingContext(sc, Seconds(5))


//    // 消费kafka数据
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
//        ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//      )


    //4、采用zookeeper手动维护偏移量
    val zkManager = new KafkaOffsetZKManager(zkHosts)

    val fromOffsets = zkManager.getFromOffset(topics, groupId)

    //5、创建数据流
    var stream: InputDStream[ConsumerRecord[String, String]] = null

    //6、 如果 offset 大于0，else
    if (fromOffsets.nonEmpty) {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, fromOffsets)
      )
    } else {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      println("第一次消费 Topic:" + topics.toBuffer)
    }



    //业务处理
    stream.map(_.value()).foreachRDD(rdd=>{

      rdd.cache()




      // 释放资源
      rdd.unpersist()
    })














 // 返回ssc
 ssc
  }





}
