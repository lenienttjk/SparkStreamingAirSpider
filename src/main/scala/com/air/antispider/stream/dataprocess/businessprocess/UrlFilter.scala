package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

object UrlFilter {
  /**
    * 数据清洗方法，清洗掉静态文件和垃圾数据
    * @param log
    * @param filterRuleRef
    * @return
    */
  def filterUrl(log: AccessLog, filterRuleRef: Broadcast[ArrayBuffer[String]]): Boolean = {
    // 设置标识 :用户匹配数据
    var isMatch = true
    filterRuleRef.value.foreach(str=>{
      // 匹配URL
      if(log.request.matches(str)){
        isMatch = false
      }
    })
    isMatch
  }

}

