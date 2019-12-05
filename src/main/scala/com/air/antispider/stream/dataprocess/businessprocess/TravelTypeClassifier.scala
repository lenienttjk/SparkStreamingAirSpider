package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum

/**
  * 往返标签
  */
object TravelTypeClassifier {
  /**
    * 往返 单程
    * @param log
    */
  def classifyByRefererAndRequestBody(log: AccessLog) = {
    // 定义日期的个数
    var dateCounts = 0
    // 定义日期正则
    val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"
    // 判断当前日期RUL是否有前后缀(是否携带参数)
    if(log.http_referer.contains("?")&&log.http_referer.split("\\?").length>1){
      // 切分数据
      val params = log.http_referer.split("\\?")(1).split("&")
      // 循环里面的值
      for (p <-params){
        // 按照'='截取
        val KV = p.split("=")
        // 匹配正则 （日期）
        if(KV(1).matches(regex)){
          // 累加
          dateCounts = dateCounts + 1
        }
      }
    }
//    // 使用URI解析
//    val uri = Url.parse(log.http_referer)
//    // 将?后面的所有参数解析
//    val paramMap = uri.query.paramMap
//    paramMap.foreach(t=>{
//      t._2.foreach(ite=>{
//        if(ite.matches(regex)){
//          dateCounts = dateCounts + 1
//        }
//      })
//    })
    // 返回标签
    if(dateCounts ==1){
      // 单程
      TravelTypeEnum.OneWay
    }else if(dateCounts == 2){
      // 往返
      TravelTypeEnum.RoundTrip
    }else{
      // 其他
      TravelTypeEnum.Unknown
    }
  }

}
