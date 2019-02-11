package Traffic.bjsxt.util

import Traffic.bjsxt.conf.ConfigurationManager
import Traffic.bjsxt.constant.Constants
import com.alibaba.fastjson.JSONObject




/**
  * 参数工具类
  */
object ParamUtils {
  /**
    * 从命令行参数中提取任务id
    * @param args 命令行参数
    * @param taskType 参数类型（任务id对应的值是long类型才可以，对应my.properties中的key）
    * @return 任务id
    */
  def getTaskIdFromArgs(args:Array[String], taskType:String) = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

    if (local) {
      ConfigurationManager.getLong(taskType)
    }else {
      if (args != null && args.length > 0) {
        args(0).toLong
      }
    }
    0L
  }

  /**
    * 从JSON对象中提取参数
    * @param jsonObject JSON对象
    * @param field
    * @return 参数
    */
  def getParam(jsonObject:JSONObject, field:String) = {
    val jsonArray = jsonObject.getJSONArray(field)
    if (jsonArray != null && jsonArray.size() > 0) {
      jsonArray.getString(0)
    }else {
      null
    }
  }
}