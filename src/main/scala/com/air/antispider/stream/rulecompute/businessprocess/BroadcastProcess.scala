package com.air.antispider.stream.rulecompute.businessprocess

import com.air.antispider.stream.common.bean.FlowCollocation
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 更新广播变量
  */
object BroadcastProcess {

  /**
    * 监控 查询关键页面 规则是否变化
    * @param sc
    * @param broadcastQueryCriticalPages
    * @param jedis
    * @return
    */
  def monitorQueryCriticalPagesRule(sc: SparkContext,
                                    broadcastQueryCriticalPages: Broadcast[ArrayBuffer[String]],
                                    jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {

    // 查询redis中的标识是否发生改变
    val needUpdateFiletList = jedis.get("broadcastQueryCriticalPagesFlag")

    // 判断标识是否为空,或者是否发生改变
    if(needUpdateFiletList!=null && !needUpdateFiletList.isEmpty && needUpdateFiletList.toBoolean){

      // 查询数据库规则
      val filterRuleListUpdate = AnalyzeRuleDB.queryCriticalPages()

      // 删除广播变量中的值
      broadcastQueryCriticalPages.unpersist()
      // 重新设置标识
      jedis.set("FilterChangeFlag","false")
      // 重新广播
      sc.broadcast(filterRuleListUpdate)
    }else{
      // 如果没有更新，就放回当前的广播变量
      broadcastQueryCriticalPages
    }



  }

  /**
    * 监控 黑名单 规则是否变化
    * @param sc
    * @param broadcastIPList
    * @param jedis
    * @return
    */
  def monitorIpBlackListRule(sc: SparkContext,
                             broadcastIPList: Broadcast[ArrayBuffer[String]],
                             jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {

    // 查询redis中的标识是否发生改变
    val needUpdateFiletList = jedis.get("IpBlackRuleChangeFlag")

    // 判断标识是否为空,或者是否发生改变
    if(needUpdateFiletList!=null && !needUpdateFiletList.isEmpty && needUpdateFiletList.toBoolean){

      // 查询数据库规则
      val filterRuleListUpdate = AnalyzeRuleDB.queryIpBlackList()

      // 删除广播变量中的值
      broadcastIPList.unpersist()
      // 重新设置标识
      jedis.set("FilterChangeFlag","false")
      // 重新广播
      sc.broadcast(filterRuleListUpdate)
    }else{
      // 如果没有更新，就放回当前的广播变量
      broadcastIPList
    }




  }

  /**
    *  监控 流程配置规则
    *
    * @param sc
    * @param broadcastFlowList
    * @param jedis
    * @return
    */
  def monitorFlowListRule(sc: SparkContext,
                          broadcastFlowList: Broadcast[ArrayBuffer[FlowCollocation]],
                             jedis: JedisCluster): Broadcast[ArrayBuffer[FlowCollocation]] = {

    // 查询redis中的标识是否发生改变
    val needUpdateFiletList = jedis.get("QueryFlowChangeFlag")

    // 判断标识是否为空,或者是否发生改变
    if(needUpdateFiletList!=null && !needUpdateFiletList.isEmpty && needUpdateFiletList.toBoolean){

      // 查询数据库规则
      val filterRuleListUpdate = AnalyzeRuleDB.createFlow(0)

      // 删除广播变量中的值
      broadcastFlowList.unpersist()
      // 重新设置标识
      jedis.set("FilterChangeFlag","false")
      // 重新广播
      sc.broadcast(filterRuleListUpdate)
    }else{
      // 如果没有更新，就放回当前的广播变量
      broadcastFlowList
    }

  }




}
