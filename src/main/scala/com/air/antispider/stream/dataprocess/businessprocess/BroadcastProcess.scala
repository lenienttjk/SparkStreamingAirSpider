package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.AnalyzeRule
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 更新广播变量
  */
object BroadcastProcess {

  /**
    * 更新过滤规则
    *
    * @param sc
    * @param filterRuleRef
    * @param jedis
    * @return
    */
  def mointorBroadFilterRule(sc: SparkContext,
                             filterRuleRef: Broadcast[ArrayBuffer[String]],
                             jedis: JedisCluster) = {

    // 查询redis中的标识是否发生改变
    val needUpdateFiletList = jedis.get("FilterChangeFlag")
    // 判断标识是否为空,或者是否发生改变
    if(needUpdateFiletList!=null &&
      !needUpdateFiletList.isEmpty &&
      needUpdateFiletList.toBoolean){
      // 查询数据库规则
      val filterRuleListUpdate = AnalyzeRuleDB.queryFilterRule()
      // 删除广播变量中的值
      filterRuleRef.unpersist()
      // 重新设置标识
      jedis.set("FilterChangeFlag","false")
      // 重新广播
      sc.broadcast(filterRuleListUpdate)
    }else{
      // 如果没有更新，就放回当前的广播变量
      filterRuleRef
    }



  }

  /**
    * 更新分类规则
    * @param sc
    * @param broadcastRuleMap
    * @param jedis
    */
  def mointorClassiferRule(sc: SparkContext,
                           broadcastRuleMap: Broadcast[util.Map[String, ArrayBuffer[String]]],
                           jedis: JedisCluster) = {
    //请求分类规则变更标识
    val needUpdateClassifyRule = jedis.get("ClassifyRuleChangeFlag")
    //Mysql-规则是否改变标识
    if (!needUpdateClassifyRule.isEmpty() && needUpdateClassifyRule.toBoolean) {
      // 查询规则
      val ruleMapTemp = AnalyzeRuleDB.queryRuleMap()
      // 释放广播变量
      broadcastRuleMap.unpersist()
      // 设置标识
      jedis.set("ClassifyRuleChangeFlag", "false")
      // 重新广播
      sc.broadcast(ruleMapTemp)
    }else{
      // 如果没有更新 返回当前广播变量
      broadcastRuleMap
    }
  }

  /**
    * 解析规则更新
    * @param sc
    * @param broadcastQueryBookRule
    * @param jedis
    * @return
    */
  def mointorQueryBooksRule(
                             sc: SparkContext,
                             broadcastQueryBookRule: Broadcast[mutable.HashMap[String, List[AnalyzeRule]]],
                             jedis: JedisCluster): Broadcast[mutable.HashMap[String, List[AnalyzeRule]]] = {
    //请求规则变更标识
    val needUpdateAnalyzeRule = jedis.get("AnalyzeRuleChangeFlag")
    //Mysql-规则是否改变标识
    if (!needUpdateAnalyzeRule.isEmpty() && needUpdateAnalyzeRule.toBoolean) {
      // 查询规则
      val queryRules = AnalyzeRuleDB.queryRule(0)
      // 预定规则
      val bookRules = AnalyzeRuleDB.queryRule(1)
      // 定义一个Map将数据存储
      val queryBooks = new mutable.HashMap[String,List[AnalyzeRule]]()
      queryBooks.put("queryRules",queryRules)
      queryBooks.put("bookRules",bookRules)
      // 释放广播变量
      broadcastQueryBookRule.unpersist()
      // 设置标识
      jedis.set("AnalyzeRuleChangeFlag", "false")
      // 重新广播
      sc.broadcast(queryBooks)
    }else{
      // 如果没有更新 返回当前广播变量
      broadcastQueryBookRule
    }
  }

  /**
    * 更新Ip黑名单
    * @param sc
    * @param ipBlackListRef
    * @param jedis
    * @return
    */
  def mointorBlackListRule(
                            sc: SparkContext,
                            ipBlackListRef: Broadcast[ArrayBuffer[String]],
                            jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    //请求规则变更标识
    val needUpdateIpBlackRule = jedis.get("IpBlackRuleChangeFlag")
    //Mysql-规则是否改变标识
    if (!needUpdateIpBlackRule.isEmpty() && needUpdateIpBlackRule.toBoolean) {
      // 查询规则
      val ipBlackList = AnalyzeRuleDB.queryIpBlackList()
      // 释放广播变量
      ipBlackListRef.unpersist()
      // 设置标识
      jedis.set("IpBlackRuleChangeFlag", "false")
      // 重新广播
      sc.broadcast(ipBlackList)
    }else{
      // 如果没有更新 返回当前广播变量
      ipBlackListRef
    }
  }


}
