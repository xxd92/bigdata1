package cleanlog.util

import java.net.URLDecoder

import cleanlog.bean.Log


/**
  * description: spark清洗器中处理URL部分的工具类
  * author: Mmy
  * create: 2018-08-09 16:24
  **/
object URLUtil {
  // 定义爬虫的类型
  val robotAry:Array[String] = Array[String]("AdsBot-Google",
    "3232baidu Transcoder", "Baiduspider", "bingbot",
    "Bloglines subscriber", "Charlotte 0.9t", "Charlotte 1.1",
    "DotBot 1.1", "eCairn Grabber 1.0", "FeedFetcher-Google",
    "Googlebot 2.1", "Java VM 1.4", "Java VM 1.5", "Java VM 1.6",
    "LinkWalker", "LiteFinder 1.0", "Mediapartners-Google",
    "msnbot 1.1", "msnbot-media 1.0", "msnbot-media 1.1",
    "msnbot/2.0b", "QihooBot", "Sogou head spider 3.0",
    "Sogou web spider 4.0", "Sosoimagespider", "Sosospider",
    "Speedy Spider",  "Superdownloads Spiderman", "WebAlta Crawler 2.0",
    "WukongSpider", "Yahoo! Slurp", "Yahoo! Slurp 3.0",
    "Yahoo! Slurp China", "Yeti 1.0", "YodaoBot 1.0",
    "YodaoBot-Image/1.0", "YoudaoBot 1.0")

  /**
    * 清洗URL部分
    * @param urlStr
    * @param log
    */
  def setLogOther(urlStr:String,log: Log): Log = {
    //先将请求参数中的错误码进行转换
    val decodedURL = if (urlStr.contains("\\x")){
      if (urlStr.contains("%u")){
        URLDecoder.decode(URLUtil.decodeCharU(urlStr.replace("\\x", "%")),"UTF-8");//使用指定的编码机制对 application/x-www-form-urlencoded 字符串解码
      }else{
        URLUtil.decodeCharU(URLDecoder.decode(urlStr.replace("\\x", "%"),"UTF-8"));
      }
    }else{
      if (urlStr.contains("%u")){
        URLDecoder.decode(URLUtil.decodeCharU(urlStr),"UTF-8");
      }else{
        URLUtil.decodeCharU(URLDecoder.decode(urlStr,"UTF-8"));
      }
    }
    // 判断传进来的u第三部分url是否为空，url的长度是否大于0
    if ((decodedURL != null) && (decodedURL.length() > 0)) {
      //url清洗后按照&符号进行切割
      val items = decodedURL.split("&")
      for (argAndValueStr <- items){
        //当字符串最后一位或者N位是分隔符时，前者split(",")不会继续切分，而后者继续切分。即前者不保留null值，后者保留。
        var argAngValuePair = argAndValueStr.split("=",-1)
        argAngValuePair(0) match {
          case "GET /images/blank.gif?unit_id" => log.unit_id = argAngValuePair(1)
          case "url" => log.url = argAngValuePair(1)
          case "urlname" => log.urlname = argAngValuePair(1)
          case "title" => log.title = argAngValuePair(1)
          case "chset" => log.chset = argAngValuePair(1)
          case "scr" => log.scr = argAngValuePair(1)
          case "col" => log.col = argAngValuePair(1)
          case "lg" => log.lg = argAngValuePair(1)
          case "je" => log.je = argAngValuePair(1)
          case "ec" => log.ec = argAngValuePair(1)
          case "fv" => log.fv = argAngValuePair(1)
          case "cnv" => log.cnv = argAngValuePair(1)
          case "ref" => log.ref = argAngValuePair(1)
          case "uagent" => log.uagent = argAngValuePair(1)
          case "stat_ss" => log.stat_ss = argAngValuePair(1)
          case "stat_uv" => log.stat_uv = argAngValuePair(1)
          case "cust_id" => log.cust_id = argAngValuePair(1)
          case "seaval" => log.seaval = argAngValuePair(1)
          case "mchid" => log.mchid = argAngValuePair(1)
          case "plevl1" => log.plevl1 = argAngValuePair(1)
          case "plevl2" => log.plevl2 = argAngValuePair(1)
          case "plevl3" => log.plevl3 = argAngValuePair(1)
          case "chlid" => log.chlid = argAngValuePair(1)
          case "prdname" => log.prdname = argAngValuePair(1)
          case "prdid" => log.prdid = argAngValuePair(1)
          case "prdcatid1" => log.prdcatid1 = argAngValuePair(1)
          case "prdcatid2" => log.prdcatid2 = argAngValuePair(1)
          case "prdcatid3" => log.prdcatid3 = argAngValuePair(1)
          case "pageId" => log.pageId = argAngValuePair(1)
          case "columnId" => log.columnId = argAngValuePair(1)
          case "columnCatId" => log.columnCatId = argAngValuePair(1)
          case "sessionId" => log.sessionId = argAngValuePair(1)
          case "moduleId" => log.moduleId = argAngValuePair(1)
          case "vendorid" => log.vendorid = argAngValuePair(1)
          case _ =>
        }
      }
    }else{
      return null
    }
    //判断是否是爬虫爬取的数据，如果是则返回null
    if (isRobotOrNot(log.uagent)){
      return null
    }
    return log
  }

  /**
    * 判断给定的值是否是爬虫
    * @param browser
    * @return
    */
  def isRobotOrNot(browser: String) = {
    // 循环数组
    for (robot <- robotAry) {
      // 判断传入进来的值是否可以找到，判断长度是否为0
      if (browser.indexOf(robot) >= 0) {
        // 返回true
        true
      }
    }
    // 返回false
    false
  }

  /**
    * 将字符中的"%u"拼接出来
    * @param src 需要转码的字符串
    * @return 返回转码后的新字符串
    */
  def decodeCharU(src: String) = {
    val buffer = new StringBuffer()
    // 确保buffer的长度大于等于转码前的字符串的长度
    buffer.ensureCapacity(src.length())
    // 定义下标变量
    var lastPos = 0
    var pos = 0
    while (lastPos < src.length()) {
      // 查找以lastPos这个位置开始，"%u"出现的位置
      pos = src.indexOf("%u", lastPos)
      if (pos == lastPos) { //
        val ch = Integer.parseInt(
          src.substring(pos + 2, pos + 6), 16).toChar //Integer.parseInt(str,16)这个是把16进制字符串转10进制int型
        buffer.append(ch)
        lastPos = pos + 6
      } else if (pos == -1) { // 证明字符串中已经没有"%u"了
        buffer.append(src.substring(lastPos))
        lastPos = src.length()
      } else {
        buffer.append(src.substring(lastPos, pos))
        lastPos = pos
      }
    }
    buffer.toString()
  }
}