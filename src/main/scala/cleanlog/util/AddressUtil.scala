package cleanlog.util

/**
  * description: 处理字符串的工具类
  * author: Mmy
  * create: 2018-08-10 14:36
  **/
object AddressUtil {
  /**
    * 通过一个地址字符串获取其所在的国、省、市、县
    * @param addressStr
    * @return
    */
  def get4PartFrom1AddressStr(addressStr:String):(String,String,String,String)={
    var country = ""
    var province = ""
    var city = ""
    var county = ""
    // 此数据中包含国，长度为1
    if (addressStr == null || "".equals(addressStr)) {
      country = "未知地区"
      province = "未知地区"
      city = "未知地区"
      county = "未知地区"
    } else {
      if ((addressStr.contains("国")) && (addressStr.split("国").length == 1)) {
        if (addressStr.split("国")(0).equals("中")) {
          country = "中国"
          province = "未知地区"
          city = "未知地区"
          county = "未知地区"
        } else {
          country = "未知地区"
          province = "未知地区"
          city = "未知地区"
          county = "未知地区"
        }
      } else if (addressStr.split("省").length == 0) {
        country = "未知地区"
        province = "未知地区"
        city = "未知地区"
        county = "未知地区"
      } else if (addressStr.split("省").length == 1) {
        country = "中国"
        province = addressStr.split("省")(0)
        city = "未知地区"
        county = "未知地区"
      } else {
        country = "中国"
        province = addressStr.split("省")(0)
        if ((addressStr.contains("市"))
          && (addressStr.split("省")(1).split("市").length == 1)) {
          city = addressStr.split("省")(1).split("市")(0) + "市"
          county = "未知地区"
        } else if ((addressStr.contains("县")) && (addressStr.contains("省"))
          && (!addressStr.contains("市")) && (!addressStr.contains("盟"))
          && (!addressStr.contains("地区"))
          && (!addressStr.contains("自治州"))) {
          city = "未知地区"
          county = "未知地区"
        } else if ((addressStr.contains("自制州"))
          && (addressStr.split("省")(1).split("自制州").length == 1)) {
          city = addressStr.split("省")(1).split("自制州")(0) + "自制州"
          county = "未知地区"
        } else if ((addressStr.contains("盟"))
          && (addressStr.split("省")(1).split("盟").length == 1)) {
          city = addressStr.split("省")(1).split("盟")(0) + "盟"
          county = "未知地区"
        } else if ((addressStr.contains("地区"))
          && (addressStr.split("省")(1).split("地区").length == 1)) {
          city = addressStr.split("省")(1).split("地区")(0) + "地区"
          county = "未知地区"
        } else if ((addressStr.contains("市"))
          && (addressStr.split("省")(1).contains("市辖区"))) {
          city = addressStr.split("省")(1).split("市")(0) + "市"
          county = "市辖区"
        } else if ((addressStr.contains("市"))
          && (!addressStr.split("省")(1).contains("市辖区"))
          && (addressStr.split("省")(1).split("市").length == 2)) {
          city = addressStr.split("省")(1).split("市")(0) + "市"
          county = addressStr.split("省")(1).split("市")(1)
        } else {
          if ((addressStr.contains("自制州"))
            && (addressStr.split("省")(1).split("自制州").length == 2)) {
            city = addressStr.split("省")(1).split("自制州")(0) + "自制州"
            county = addressStr.split("省")(1).split("自制州")(1)
          }
          if ((addressStr.contains("盟"))
            && (addressStr.split("省")(1).split("盟").length == 2)) {
            city = addressStr.split("省")(1).split("盟")(0) + "盟"
            county = addressStr.split("省")(1).split("盟")(1)
          }
          if ((addressStr.contains("地区"))
            && (addressStr.split("省")(1).split("地区").length == 2)) {
            city = addressStr.split("省")(1).split("地区")(0) + "地区"
            county = addressStr.split("省")(1).split("地区")(1)
          }
        }
      }
    }
    (country,province,city,county)
  }

}
