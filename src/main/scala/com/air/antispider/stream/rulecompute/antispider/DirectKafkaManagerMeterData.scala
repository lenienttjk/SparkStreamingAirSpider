package com.air.antispider.stream.rulecompute.antispider

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * spark-streaming读取kafka
  * 适用本版：spark-streaming-kafka-0-10
  *
  * 实时处理主类
  */
object DirectKafkaManagerMeterData {

  def main(args: Array[String]): Unit = {

    //设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)

    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")


    // 1、初始化
    val conf = new SparkConf().setAppName("DirectKafkaManagerMeterData").setMaster("local[4]")

    // 1.5 及以后 动态限流
    conf.set("spark.streaming.backpressure", "true")


    val ssc = new StreamingContext(conf, Seconds(5))


    //2、kafka 参数
    val BOOTSTRAP_SERVER = "mini1:9092,mini2:9092,mini3:9092"
    val ZK_SERVERS = "mini1:2181,mini2:2181,mini3:2181"
    val GROUP_ID = "g1"
    val topics = Set("processedQuery")

    //3、 kafkaParams 参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVER,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 指定读取kafak 的数据的编码
      //      "deserializer.encoding" -> "GB2312",
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )




    //4、采用zookeeper手动维护偏移量
    val zkManager = new KafkaOffsetZKManager(ZK_SERVERS)
    val fromOffsets = zkManager.getFromOffset(topics, GROUP_ID)

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


    //7、处理流数据，业务处理在这里
    stream.foreachRDD { rdd =>
      // 7.1 转换
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 7.1 处理  （offset，partition，value）
      val rs = rdd.map(record => (record.offset(), record.partition(), record.value())).collect()




      rdd.foreachPartition(partitions => {

        partitions.foreach(line => {
          //处理数据逻辑  等等逻辑





          // 输出topic 信息
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          System.err.println("Topic：" + o.topic + ", Partition: " + o.partition + ", FromOffset: " + o.fromOffset + ", UpdatedOffset: " + o.untilOffset + ", value: " + line.value())


        })
      })

      // 9、处理完数据后 更新偏移量
      zkManager.storeOffsets(offsetRanges, GROUP_ID)
    }

    // 10、启动
    ssc.start()
    ssc.awaitTermination()
  }

  /*

    * 过程时间

    */

  class Stopwatch {

    private val start = System.currentTimeMillis()

    override def toString() = (System.currentTimeMillis() - start) + " ms"

  }



}
