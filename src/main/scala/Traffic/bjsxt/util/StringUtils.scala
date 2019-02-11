package Traffic.bjsxt.util

import scala.collection.immutable.HashMap

/**
  * 字符串工具类
  */
object StringUtils {
  /**
    * 判断字符串是否为空
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str:String) = {
    str == null || "".equals(str)
  }

  /**
    * 判断字符串是否不为空
    * @param str 字符串
    * @return 是否不为空
    */
  def isNotEmpty(str:String) = {
    str != null && !"".equals(str)
  }

  /**
    * 截断字符串两侧的逗号
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str:String) = {
    var s = ""
    if (str.startsWith(",")) {
      s = str.substring(1)
    }
    if (str.endsWith(",")) {
      s = str.substring(0,str.length - 1)
    }
    s
  }

  /**
    * 补全两位数字
    * @param str
    * @return
    */
  def fulfuill(str:String) = {
    if (str.length == 1)
      "0" + str
    else
      str
  }

  /**
    * 补全num为数字
    * 将给定的字符串前面补0，使字符串的长度为num为
    * @param num 补全后的长度
    * @param str 要补全的字符串
    * @return
    */
  def fulfuill(num:Int, str:String) = {
    if (str.length == num) {
      str
    } else {
      val fulNum = num - str.length
      var tmpStr = ""
      for (i <- 0 until fulNum) {
        tmpStr += "0"
      }
      tmpStr + str
    }
  }

  /**
    * 从拼接的字符串中提取字段
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段值
    */
  def getFieldFromConcatString(str:String, delimiter:String, field:String) = {
    //根据传入的分隔符分隔
    val fields = str.split(delimiter)
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        //判读是否与传入的字段名相同
        if (fieldName.equals(field)) {
          fieldValue
        }
      }
    }
  }

  /**
    * 从拼接的字符串中给字段设置值
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str:String, delimiter:String, field:String, newFieldValue:String) = {
    val fields = str.split(delimiter)
    for (i <- 0 until fields.length) {
      val fieldName = fields(i).split("=")(0)
      if (fieldName.equals(field)) {
        val concatField = fieldName + "=" + newFieldValue
        fields(i) = concatField
      }
    }

    val buffer = new StringBuffer("")
    for(i <- 0 until fields.length) {
      buffer.append(fields(i))
      if (i < fields.length - 1 ) {
        buffer.append("|")
      }
    }
    buffer.toString
  }

  /**
    * 给定字符串和分隔符，返回一个k，v map
    * @param str
    * @param delimiter
    * @return
    */
  def getKeyValuesFromConcatString(str:String, delimiter:String) = {
    var map:Map[String,String] = new HashMap()
    val fields = str.split(delimiter)
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        map.+= (fieldName -> fieldValue)
      }
    }
    map
  }

  /**
    * String 字符串转Integer数字
    * @param str
    * @return
    */
  def convertStringtoInt(str:String) = {
    Integer.parseInt(str)
  }
}