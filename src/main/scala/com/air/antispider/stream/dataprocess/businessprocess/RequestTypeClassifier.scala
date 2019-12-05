package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.ArrayBuffer

/**
  * 分类打标签
  */
object RequestTypeClassifier {
  /**
    * 根据request RUL进行打标记
    * 国内查询(0,0) 国内预定(0,1) 国际查询(1,0) 国际预定(1,1)
    * (-1,-1) 其他
    * @param request
    * @param map
    */
  def classifyByRequest(request: String,
                        map: util.Map[String, ArrayBuffer[String]]) = {

    // 获取数据库的正则

    // 获取国内查询
    val nqArr = map.get("nq")
    // 国际查询
    val iqArr = map.get("iq")
    // 国内预定
    val nbArr = map.get("nb")
    // 国际预定
    val ibArr = map.get("ib")
    // 可变变量
    var flag = true
    // 请求参数
    var requestType:RequestType = null
    // 国内查询正则
    nqArr.foreach(rule=>{
      // 匹配
      if(request.matches(rule)){
        // 匹配上，设置为false，不用在打其他标记
        flag =false
        // 打标记 (0,0)
        requestType = RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Query)
      }
    })
    // 国际查询正则
    iqArr.foreach(rule=>{
      // 匹配
      if(request.matches(rule)){
        // 匹配上，设置为false，不用在打其他标记
        flag =false
        // 打标记 (1,0)
        requestType = RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Query)
      }
    })
    // 国内预定正则
    nbArr.foreach(rule=>{
      // 匹配
      if(request.matches(rule)){
        // 匹配上，设置为false，不用在打其他标记
        flag =false
        // 打标记 (0,1)
        requestType = RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Book)
      }
    })
    // 国际预定正则
    ibArr.foreach(rule=>{
      // 匹配
      if(request.matches(rule)){
        // 匹配上，设置为false，不用在打其他标记
        flag =false
        // 打标记 (1,1)
        requestType = RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Book)
      }
    })
    // 如果什么都没匹配上，返回(-1,-1)
    if(flag){
      requestType = RequestType(FlightTypeEnum.Other,BehaviorTypeEnum.Other)
    }
    // 返回结果
    requestType
  }

}
