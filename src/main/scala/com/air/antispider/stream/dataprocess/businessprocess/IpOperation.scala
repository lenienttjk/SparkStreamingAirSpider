package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer

object IpOperation {
  /**
    * 高频Ip匹配
    * @param ip
    * @param value
    */
  def isFreIP(ip: String, value: ArrayBuffer[String]):Boolean = {
    // 初始化标记
    var flag = false
    // 从广播变量获取黑名单 进行匹配
    val iter = value.iterator
    while (iter.hasNext){
      val str = iter.next()
      // 匹配数据
      if(str.eq(ip)){
        flag =true
      }
    }
    flag
  }

}
