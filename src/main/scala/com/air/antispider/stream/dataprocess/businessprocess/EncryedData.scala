package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.Pattern

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.decode.MD5

object EncryedData {

  /**
    * 数据脱敏 手机号
    *
    * @param log
    * @return
    */
  def encryptedPhone(log: String):String= {
    // 获取MD5加密
    val md5 = new MD5
    // 获取http_cookie
    var cookie = log
    // 手机号正则表达式
    val phonePattern = Pattern
      .compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
    // 匹配手机号
    val phoneMatcher = phonePattern.matcher(cookie)
    // 循环处理手机号（多个）
    while (phoneMatcher.find()){
      // 拿到手机号后，获取手机号的前一个字符和后一个字符
      // 会因为我们拿来的数据是由前缀和后缀“=”，“:”这样的东西，所以需要根据数据进行判断处理
      // 手机号的前一个index
      val lowIndex = log.indexOf(phoneMatcher.group())-1
      // 手机号的后一个index
      val highIndex = lowIndex+phoneMatcher.group().length + 1
      // 手机号的前一个字符
      val lowLetter = log.charAt(lowIndex).toString
      // 匹配当前第一位不是数字
      if(!lowLetter.matches("^[0-9]$")){
        // 如果字符串的最后是手机号，直接替换即可
        if(highIndex < log.length){
          // 拿到手机号的最后一个字符
          val highLetter = log.charAt(highIndex).toString
          // 后一位也不是数字，说明这个字符串就是一个手机号
          if(!highLetter.matches("^[0-9]$")){
            // 直接替换
            cookie = cookie.replace(phoneMatcher.group(),md5.getMD5ofStr(phoneMatcher.group()))
          }
        }
      }
    }



    cookie
  }
  // 身份证号
  def encryptedId(log: String): String = {
    // 获取MD5加密
    val md5 = new MD5
    // 使用局部变量
    var cookie = log
    // 身份证号正则 匹配两种身份证长度 18 或 15
    val idPattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")
    // 匹配身份证号
    val idMatcher = idPattern.matcher(log)
//    // 循环处理
    while (idMatcher.find()){
      val lowIndex = log.indexOf(idMatcher.group())-1
      // 身份证号的后一个index
      val highIndex = lowIndex+idMatcher.group().length + 1
      // 身份证号的前一个字符
      val lowLetter = log.charAt(lowIndex).toString
      // 匹配当前第一位不是数字
      if(!lowLetter.matches("^[0-9]$")){
        // 如果字符串的最后是身份证号，直接替换即可
        if(highIndex < log.length){
          // 拿到身份证号的最后一个字符
          val highLetter = log.charAt(highIndex).toString
          // 后一位也不是数字，说明这个字符串就是一个身份证号
          if(!highLetter.matches("^[0-9]$")){
            // 直接替换
            cookie = cookie.replace(idMatcher.group(),md5.getMD5ofStr(idMatcher.group()))
          }
        }
      }
    }
    cookie
  }
}
