package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum

object DataPackage {
  /**
    * 封装结构化数据
    *
    * @param str
    * @param log
    * @param highFrqIPGroup
    * @param requestTypeLabel
    * @param travelType
    * @param queryRequestData
    * @param bookRequestData
    * @return
    */
  def dataPackage(
                   str: String,
                   log: AccessLog,
                   highFrqIPGroup: Boolean,
                   requestTypeLabel: RequestType,
                   travelType: TravelTypeEnum.Value,

                   queryRequestData: Option[QueryRequestData],
                   bookRequestData: Option[BookRequestData],

                   httpReferrer: String) = {
    // 飞行时间
    var flightData = ""
    bookRequestData match {
      case Some(book) => flightData = book.flightDate.mkString
      case None => println("Null")
    }
    queryRequestData match {
      case Some(query) => flightData = query.flightDate
      case None => println("Null")
    }

    // 始发地
    var depCity = ""
    bookRequestData match {
      case Some(book) => depCity = book.depCity.mkString
      case None => println("Null")
    }
    queryRequestData match {
      case Some(query) => depCity = query.depCity
      case None => println("Null")
    }

    // 目的地
    var arrCity = ""
    bookRequestData match {
      case Some(book) => arrCity = book.arrCity.mkString
      case None => println("Null")
    }
    queryRequestData match {
      case Some(query) => arrCity = query.arrCity
      case None => println("Null")
    }
    // 封装这些主要参数
    val params = CoreRequestParams(flightData, depCity, arrCity)
    // 将参数放入样例类  ProcessedData
    ProcessedData(
      str,
      log.request_method,
      log.request,
      log.remote_addr,
      log.http_user_agent,
      log.time_iso8601,
      log.server_addr,
      highFrqIPGroup,
      requestTypeLabel,
      travelType, params,
      log.jessionID,
      log.userID,
      queryRequestData,
      bookRequestData,
      log.http_referer)
  }

}

