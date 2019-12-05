package com.air.antispider.stream.rulecompute.businessprocess


import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


/*

  * 反爬核心规则函数集

  */

object CoreRule {


  /*

    *  单位时间内的IP段  访问量（前两位）

    *

    * @param structuredDataLines  结构化的查询数据

    * @param windowDuration      窗口长度 9

    * @param slideDuration       窗口滑动间隔 3

    * @return DStream[(String, Int)] 返回值

    */

  def ipBlockAccessCounts(structuredDataLines: DStream[ProcessedData],

                          windowDuration: Duration,

                          slideDuration: Duration): DStream[(String, Int)] = {

    //获取IP字段
    structuredDataLines.map { processedData =>

      //remoteaddr没数据

      if ("NULL".equalsIgnoreCase(processedData.remoteAddr)) {

        (processedData.remoteAddr, 1)

      } else {

        //remoteaddr有数据,如192.168.56.151第一个“.”出现的位置

        val index = processedData.remoteAddr.indexOf(".")

        try {

          /*

            *processedData.remoteAddr.indexOf(".", index + 1)：找“.”，从index+1位置查找，也就是找第二个“.”的位置

            * 计算结果如：(192.168,10)

            * 计算结果如：(192.156,16)

            */

          // 从 0 substring 截取 到第二个“.” 位置
          (processedData.remoteAddr.substring(0, processedData.remoteAddr.indexOf(".", index + 1)), 1)

        } catch {

          case e: Exception =>
            e.printStackTrace()
            ("", 1)

        }

      }

      //叠加5分钟的数据

    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)

  }


  /*

    * 某个IP，单位时间内总访问量

    *

    * @param structuredDataLines ProcessedData数据

    * @param windowDuration      窗口长度

    * @param slideDuration       窗口滑动周期

    * @return DStream[(String, Int)]

    */

  def ipAccessCounts(structuredDataLines: DStream[ProcessedData],

                     windowDuration: Duration,

                     slideDuration: Duration): DStream[(String, Int)] = {

    structuredDataLines.map { processedData =>

      (processedData.remoteAddr, 1)

    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)

  }


  /*

      * 某个IP，单位时间内的关键页面访问总量

      *

      * @param structuredDataLines ProcessedData数据

      * @param windowDuration      窗口长度

      * @param slideDuration       窗口滑动周期

      * @return DStream[(String, Int)]

      1、 拿到kafka消息和数据库关键页面广播变量的value

      2、 从kafka数据中拿到request，匹配关键页面

      3、 匹配成功记录（remoteaddr，1）

      4、 匹配失败记录（remoteaddr，0）

      */

  def criticalPagesCounts(structuredDataLines: DStream[ProcessedData],

                          windowDuration: Duration,

                          slideDuration: Duration,

                          queryCriticalPages: ArrayBuffer[String]): DStream[(String, Int)] = {

    structuredDataLines.map { processedData =>

      //从request中拿到访问页面

      val request = processedData.request

      //判断页面是否为关键页面

      var flag = false

      for (page <- queryCriticalPages) {

        if (request.matches(page)) {

          flag = true

        }

      }

      //如果是关键页面，记录这个ip为1

      if (flag) {
        (processedData.remoteAddr, 1)
      } else {
        //如果不是关键页面，记录为0
        (processedData.remoteAddr, 0)

      }

      //统计
    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)

  }



  /*

  * 某个IP，单位时间内的UA种类数统计

  * @param structuredDataLines ProcessedData数据

  * @param windowDuration      窗口长度

  * @param slideDuration       窗口滑动周期

  * @return DStream[(String, Iterable[String])]

  */

  def userAgent(structuredDataLines: DStream[ProcessedData],

                windowDuration: Duration,

                slideDuration: Duration): DStream[(String, Iterable[String])] = {

    structuredDataLines.map { processedData =>

      (processedData.remoteAddr, processedData.httpUserAgent)

    }.groupByKeyAndWindow(windowDuration, slideDuration)

  }



  /*

    * 某个IP，单位时间内的关键页面最短访问间隔

    *

    * @param structuredDataLines ProcessedData数据

    * @param windowDuration      窗口长度

    * @param slideDuration       窗口滑动周期

    * @return DStream[(String, Iterable[String])]

    */

  def criticalPagesAccTime(structuredDataLines: DStream[ProcessedData],

                           windowDuration: Duration,

                           slideDuration: Duration,

                           queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {


    structuredDataLines.map { processedData =>

      //   时间
      val accessTime = processedData.timeIso8601
       // 页面
      val request = processedData.request


      var flag = false

      for (i <- 0 until queryCriticalPages.size) {

        // 页面匹配，保留
        if (request.matches(queryCriticalPages(i))) {
          flag = true

        }

      }


      if (flag) {
        // (ip,  time)
        (processedData.remoteAddr, accessTime)

      } else {

        (processedData.remoteAddr, "0")

      }

    }.groupByKeyAndWindow(windowDuration, slideDuration)

  }







  /*


      * 统计：某个IP，单位时间内小于最短访问间隔（自设）的关键页面查询次数

      *

      * @param structuredDataLines ProcessedData数据

      * @param windowDuration      窗口长度

      * @param slideDuration       窗口滑动周期

      * @return DStream[((String, String), Iterable[String])]

      */

  def aCriticalPagesAccTime(structuredDataLines: DStream[ProcessedData],

                            windowDuration: Duration,

                            slideDuration: Duration,

                            queryCriticalPages: ArrayBuffer[String]): DStream[((String, String), Iterable[String])] = {

    structuredDataLines.map { processedData =>

      val accessTime = processedData.timeIso8601

      val request = processedData.request


      var flag = false

      for (i <- 0 until queryCriticalPages.size) {

        if (request.matches(queryCriticalPages(i))) {

          flag = true
        }
      }


      if (flag) {
        ((processedData.remoteAddr, request), accessTime)
      } else {
        ((processedData.remoteAddr, request), "0")
      }
    }.groupByKeyAndWindow(windowDuration, slideDuration)

  }


  /**
    * 某个IP ,单位时间内 查询不同行程的 次数
    *
    */
  def userAgentCount(structuredDataLines: DStream[ProcessedData],

                     windowDuration: Duration,

                     slideDuration: Duration): DStream[(String, Iterable[String])] = {

   structuredDataLines.map { processedData => (processedData.remoteAddr, processedData.httpUserAgent)

    }
    .groupByKeyAndWindow(windowDuration, slideDuration)

  }



  /**
    * 某个IP，5分钟内 不同行程的 查询次数
    *
    * @param structuredDataLines
    * @param windowDuration
    * @param slideDuration
    * @return
    */
  def flightQuerys(structuredDataLines: DStream[ProcessedData],

                   windowDuration: Duration,

                   slideDuration: Duration): DStream[(String, Iterable[(String, String)])] = {

    structuredDataLines.map { processedData =>

      (processedData.remoteAddr, (processedData.requestParams.depcity, processedData.requestParams.arrcity))

    }.groupByKeyAndWindow(windowDuration, slideDuration)

  }








  /*

      * 某个IP，关键页面访问Cookie

      *

      * @param structuredDataLines ProcessedData数据

      * @param windowDuration      窗口长度

      * @param slideDuration       窗口滑动周期

      * @return DStream[((String, String), Iterable[String])]

      */

  def criticalCookies(structuredDataLines: DStream[ProcessedData],

                      windowDuration: Duration,

                      slideDuration: Duration,

                      queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {

    structuredDataLines.map { processedData =>

      val request = processedData.request

      var flag = false

      for (i <- 0 until queryCriticalPages.size) {

        if (request.matches(queryCriticalPages(i))) {

          flag = true
        }
      }

      if (flag) {

        (processedData.remoteAddr, processedData.cookieValue_JSESSIONID)

      } else {

        (processedData.remoteAddr, "")

      }

    }.groupByKeyAndWindow(windowDuration, slideDuration)

  }


}
