package com.air.antispider.stream.rulecompute.businessprocess

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.air.antispider.stream.common.bean.{FlowCollocation, RuleCollocation}
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object AnalyzeRuleDB {


  /**

    * 查询关键页面

    * @return

    */

  def queryCriticalPages(): ArrayBuffer[String] = {

    val queryCriticalPagesSql = "select criticalPageMatchExpression from nh_query_critical_pages"

    val queryCriticalPagesField = "criticalPageMatchExpression"

    val queryCriticalPages = QueryDB.queryData(queryCriticalPagesSql, queryCriticalPagesField)

    queryCriticalPages

  }


  /*

   * 查询ip黑名单，添加到广播变量

   *

   * @return

   */

  def queryIpBlackList(): ArrayBuffer[String] = {

    //mysql中ip黑名单数据
    val nibsql = "select ip_name from nh_ip_blacklist"
    val nibField = "ip_name"
    val ipInitList = QueryDB.queryData(nibsql, nibField)
    ipInitList
  }


  /*

    * 获取流程列表

    * 参数n为0为 反爬虫流程

    *参数n为1为 防占座流程

    *

    * @return ArrayBuffer[FlowCollocation]

    */

  def createFlow(n: Int): ArrayBuffer[FlowCollocation] = {

    // 数组存放，多个字段
    var array = new ArrayBuffer[FlowCollocation]

    var sql: String = ""

    if (n == 0) {
      sql = "select nh_process_info.id,nh_process_info.process_name,nh_strategy.crawler_blacklist_thresholds from nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=0"
    }

    else if (n == 1) {
      sql = "select nh_process_info.id,nh_process_info.process_name,nh_strategy.occ_blacklist_thresholds from nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=1"
    }


    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try {

      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()

      //循环
      while (rs.next()) {

        val flowId = rs.getString("id")
        val flowName = rs.getString("process_name")


        if (n == 0) {

          val flowLimitScore = rs.getDouble("crawler_blacklist_thresholds")

          array += new FlowCollocation(flowId, flowName, createRuleList(flowId, n), flowLimitScore, flowId)

        } else if (n == 1) {

          val flowLimitScore = rs.getDouble("occ_blacklist_thresholds")

          array += new FlowCollocation(flowId, flowName, createRuleList(flowId, n), flowLimitScore, flowId)

        }
      }
    } catch {

      case e: Exception => e.printStackTrace()

    } finally {

      c3p0Util.close(conn, ps, rs)

    }

    // 返回值
    array

  }


  /*

  * 获取规则列表

  * @param process_id 根据该ID查询规则

  * @return list列表

  */

  def createRuleList(process_id: String, n: Int): List[RuleCollocation] = {

    var list = new ListBuffer[RuleCollocation]

    val sql = "select * from(select nh_rule.id,nh_rule.process_id,nh_rules_maintenance_table.rule_real_name,nh_rule.rule_type,nh_rule.crawler_type,nh_rule.status,nh_rule.arg0,nh_rule.arg1,nh_rule.score from nh_rule,nh_rules_maintenance_table where nh_rules_maintenance_table.rule_name=nh_rule.rule_name) as tab where process_id = '" + process_id + "'and crawler_type=" + n

    //and status="+n

    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try {

      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()

      while (rs.next()) {

        val ruleId = rs.getString("id")

        val flowId = rs.getString("process_id")

        val ruleName = rs.getString("rule_real_name")

        val ruleType = rs.getString("rule_type")

        val ruleStatus = rs.getInt("status")

        val ruleCrawlerType = rs.getInt("crawler_type")

        val ruleValue0 = rs.getDouble("arg0")

        val ruleValue1 = rs.getDouble("arg1")

        val ruleScore = rs.getInt("score")

        // 封装成RuleCollocation
        val ruleCollocation = new RuleCollocation(ruleId, flowId, ruleName, ruleType, ruleStatus, ruleCrawlerType, ruleValue0, ruleValue1, ruleScore)

        list += ruleCollocation

      }

    } catch {

      case e: Exception => e.printStackTrace()

    } finally {

      c3p0Util.close(conn, ps, rs)

    }

    list.toList

  }

}
