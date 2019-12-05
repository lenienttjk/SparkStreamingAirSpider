package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

object DataSend {

  /**
    * 推送query查询数据
    *
    * @param dataProcess
    */
  def sendQueryDataToKafka(dataProcess: RDD[ProcessedData]) = {

    // 过滤出query数据
    val queryDataToKafka = dataProcess
      .filter(f => f.requestType.behaviorType == BehaviorTypeEnum.Query)
      .map(x => x.toKafkaString())


    // 如果有查询数据，推送查询数据到Kafka
    if (!queryDataToKafka.isEmpty()) {
      // 获取Topic
      val queryTopic = PropertiesUtil.getStringByKey(
        "target.query.topic", "kafkaConfig.properties")
      // kafka参数配置
      val kafkaMap = new util.HashMap[String, Object]()
      // 设置brokerList
      kafkaMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
      //key序列化方法
      kafkaMap.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
      //value序列化方法
      kafkaMap.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
      //批发送设置：32KB作为一批次或10ms作为一批次
      kafkaMap.put(ProducerConfig.BATCH_SIZE_CONFIG,
        PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
      kafkaMap.put(ProducerConfig.LINGER_MS_CONFIG,
        PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))
      // 按照分区发送数据
      queryDataToKafka.foreachPartition(iter => {
        // 创建Producer
        val producer = new KafkaProducer[String, String](kafkaMap)
        iter.foreach(rdd => {
          // 发送消息
          val push = new ProducerRecord[String, String](queryTopic, null, rdd)
          producer.send(push)
        })
        // 关闭流
        producer.close()
      })
    }
  }

  /**
    * 推送book 预定数据
    *
    * @param dataProcess
    */

  //推送book数据 到kafka


  def sendBookDataToKafka(dataProcess: RDD[ProcessedData]): Unit = {

    //过滤出book的数据

    val bookDataToKafka = dataProcess.filter(y => y.requestType.behaviorType == BehaviorTypeEnum.Book).map(x => x.toKafkaString())

    //book的topic

    val bookTopic = PropertiesUtil.getStringByKey("target.book.topic", "kafkaConfig.properties")

    //推送预订数据到Kafka

    if (!bookDataToKafka.isEmpty()) {

      //创建map，封装kafka参数

      val kafkaMap = new java.util.HashMap[String, Object]()

      //设置brokers
      kafkaMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))

      //key序列化方法
      kafkaMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))

      //value序列化方法
      kafkaMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))

      //批发送设置：32KB作为一批次或10ms作为一批次
      kafkaMap.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))

      kafkaMap.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))




      //按照分区发送数据
      bookDataToKafka.foreachPartition { records =>

        //每个分区创建一个kafkaproducer,是一个k-v类型
        val producer = new KafkaProducer[String, String](kafkaMap)

        records.foreach { record =>

          //发送数据，ProducerRecord 也是一个k-v类型
          val push = new ProducerRecord[String, String](bookTopic, null, record)

          producer.send(push)

        }

        //关闭流

        producer.close()
      }
    }
  }

}
