package com.air.antispider.stream.rulecompute.antispider

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * kafka偏移量zookeeper维护类
  * 适用本版：spark-streaming-kafka-0-10
  *
  * @param zkServers zookeeper server
  */
class KafkaOffsetZKManager(zkServers: String) {

  //创建zookeeper连接客户端
  val zkClient = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString(zkServers)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      //                .namespace("kafka")//创建包含隔离命名空间的会话
      .build()
    client.start()
    client
  }

  // 设置路径
  val _base_path_of_kafka_offset = "/kafka/offsets" //offset 路径起始位置
  /**
    * 获取消费者组topic已消费偏移量（即本次起始偏移量）
    *
    * @param topics    topic集合
    * @param groupName 消费者组
    * @return
    */
  def getFromOffset(topics: Set[String], groupName: String): Map[TopicPartition, Long] = {

    // Kafka 0.8和0.10的版本差别:0.10->TopicPartition ,0.8->TopicAndPartition
    var fromOffset: Map[TopicPartition, Long] = Map()

    for (topic <- topics) {

      val topic = topics.toString

      println("topic=======" + topic)
      // 读取ZK中保存的Offset，作为Dstrem的起始位置。如果没有则创建该路径，并从 0 开始Dstream
      val zkTopicPath = s"${_base_path_of_kafka_offset}/${groupName}/${topic}"

      // 检查路径是否存在
      checkZKPathExists(zkTopicPath)

      // 获取topic的子节点，即 分区
      val childrens = zkClient.getChildren().forPath(zkTopicPath)

      // 遍历分区
      // 需要导入 scala的 包，
      import scala.collection.JavaConversions._
      for (p <- childrens) {

        // 遍历读取子节点中的数据：即 offset
        val offsetData = zkClient.getData().forPath(s"${zkTopicPath}/${p}")
        println(offsetData.toString + "----------------------------------")


        // 将offset转为Long
        val offset = java.lang.Long.valueOf(new String(offsetData)).toLong
        // fromOffset是 Map(k,v)  +=  ( TopicPartition => offset)
        fromOffset += (new TopicPartition(topic, Integer.parseInt(p)) -> offset)

      }
    }
    println(fromOffset + "+++++++++++++++++++++++++++++++++++++")
    fromOffset
  }

  /**
    * 检查ZK中路径存在，不存在则创建该路径
    *
    * @param path
    * @return
    */
  def checkZKPathExists(path: String) = {
    if (zkClient.checkExists().forPath(path) == null) {
      zkClient.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  /**
    * 保存或更新偏移量
    *
    * @param offsetRanges
    * @param groupName
    */
  def storeOffsets(offsetRanges: Array[OffsetRange], groupName: String): Unit = {

    for (o <- offsetRanges) {
      val zkPath = s"${_base_path_of_kafka_offset}/${groupName}/${o.topic}/${o.partition}"

      // 检查路径是否存在
      checkZKPathExists(zkPath)

      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset write to zookeeper------\nTopic：" + o.topic + ", Partition: " + o.partition + ",  Offset:  " + o.untilOffset)
      zkClient.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }
}