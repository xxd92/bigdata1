package cleanlog.bean

/**
  * description:Log类型，用来封装清洗后的各个属性值
  * 注意：在spark程序中使用自定义的类型时，对应的类必须可序列化
  *       即：class 类名 extends scala.Serializable
  * author: Mmy
  * create: 2018-08-05 21:07
  **/
class Log extends scala.Serializable{
  //Log 类属性，本质就是Hive表需要用户到的各个属性
  var unit_id:String = ""
  var url:String=""
  var urlname:String=""
  var title:String=""
  var chset:String=""
  var scr:String=""
  var col:String=""
  var lg:String=""
  var je:String=""
  var ec:String=""
  var fv:String=""
  var cnv:String=""
  var ref:String=""
  var uagent:String=""
  var stat_ss:String=""
  var stat_uv:String=""
  var cust_id:String=""
  var seaval:String=""
  var mchid:String=""
  var plevl1:String=""
  var plevl2:String=""
  var plevl3:String=""
  var chlid:String=""
  var prdname:String=""
  var prdid:String=""
  var prdcatid1:String=""
  var prdcatid2:String=""
  var prdcatid3:String=""
  var pageId:String=""
  var columnId:String=""
  var columnCatId:String=""
  var sessionId:String=""
  var moduleId:String=""
  var vendorid:String=""
  var clientTime:String=""
  var milliseconds:String=""
  var clientTimeYear:String=""
  var clientTimeMonth:String=""
  var clientTimeDay:String=""
  var clientTimeHour:String=""
  var clientDate:String=""
  var IP:String=""
  var country:String=""
  var province:String=""
  var city:String=""
  var county:String=""

  override def toString = s"$unit_id\001$url\001$urlname\001$title\001$chset\001$scr\001$col\001$lg\001$je\001$ec\001$fv\001$cnv\001$ref\001$uagent\001$stat_ss\001$stat_uv\001$cust_id\001$seaval\001$mchid\001$plevl1\001$plevl2\001$plevl3\001$chlid\001$prdname\001$prdid\001$prdcatid1\001$prdcatid2\001$prdcatid3\001$pageId\001$columnId\001$columnCatId\001$sessionId\001$moduleId\001$vendorid\001$clientTime\001$milliseconds\001$clientTimeYear\001$clientTimeMonth\001$clientTimeDay\001$clientTimeHour\001$clientDate\001$IP\001$country\001$province\001$city\001$county"
}
