package Traffic.bjsxt.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 日期工具类
  */
object DateUtils {
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")

  /**
    *  判断一个时间是否在另一个时间之前
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1:String, time2:String) = {
    val dateTime1 = TIME_FORMAT.parse(time1)
    val dateTime2 = TIME_FORMAT.parse(time2)

    if (dateTime1.before(dateTime2)) {
      true
    } else {
      false
    }
  }

  /**
    *  判断一个时间是否在另一个时间之后
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1:String, time2:String) = {
    val dateTime1 = TIME_FORMAT.parse(time1)
    val dateTime2 = TIME_FORMAT.parse(time2)

    if (dateTime1.after(dateTime2)) {
      true
    } else {
      false
    }
  }

  /**
    *  计算时间差值（单位为秒）
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1:String, time2:String) = {
    try{
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)

      val millisecond = dateTime1.getTime - dateTime2.getTime
      Integer.valueOf(String.valueOf(millisecond / 1000))
    } catch {
      case e:Exception => println(e)
    }finally {
      0
    }
  }

    /**
      *  获取年月日和小时
      * @param datetime 时间（YYYY-MM-dd HH:mm:ss）
      * @return 结果（yyyy-MM-dd_HH）
      */
    def getDateHour(datetime:String) = {
      //获取到年月日
      val date = datetime.split(" ")(0)
      val hourMinuteSecond = datetime.split(" ")(1)
      //获取到小时
      val hour = hourMinuteSecond.split(":")(0)
      date + "_" + hour
    }

    /**
      *  获取当天日期（yyyy-MM-dd）
      * @return 当天日期
      */
    def getTodayDate() = {
      DATE_FORMAT.format(new Date())
    }

    /**
      *  获取昨天的日期（yyyy-MM-dd）
      * @return 昨天的日期
      */
    def getYesterdatDate() = {
      //使用默认时区和语言环境获得一个日历
      val cal = Calendar.getInstance()
      //设置当前日期
      cal.setTime(new Date())
      //获取昨天的日期
      cal.add(Calendar.DAY_OF_YEAR, -1)

      val date = cal.getTime
      DATE_FORMAT.format(date)
    }

    /**
      *  格式化日期（yyyy-MM-dd）
      * @param date Date对象
      * @return 格式化后的日期
      */
    def formatDate(date:Date) = {
      DATE_FORMAT.format(date)
    }

    /**
      *  格式化时间（yyyy-MM-dd HH:mm:ss）
      * @param date Date对象
      * @return 格式化后的时间
      */
    def formatTime(date:Date) = {
      TIME_FORMAT.format(date)
    }

    /**
      *  解析时间字符串
      * @param time 时间字符串
      * @return Date
      */
    def parseTime(time:String) = {
      try {
        TIME_FORMAT.parse(time)
      }catch {
        case e:Exception => println(e)
      }finally {
        null
      }
    }

    /**
      * 格式化日期key
      * @param date
      * @return yyyyMMdd
      */
    def formatDateKey(date:Date) = {
      DATEKEY_FORMAT.format(date)
    }

    /**
      * 格式化日期key（yyyyMMdd）
      * @param datekey
      * @return
      */
    def parseDateKey(datekey:String) = {
      DATEKEY_FORMAT.parse(datekey)
    }

    /**
      * 格式化时间，保留到分钟级别
      * @param date
      * @return  yyyyMMddHHmm
      */
    def formatTimeMinute(date:Date) = {
      val sdf = new SimpleDateFormat("yyyyMMddHHmm")
      sdf.format(date)
    }

    /**
      * 获取五分钟内的时间
      * @param dateTime
      * @return
      */
    def getRangeTime(dateTime:String) = {
      //获取到年月日
      val date = dateTime.split(" ")(0)
      //获取到小时
      val hour = dateTime.split(" ")(1).split(":")(0)
      //获取到分钟并转化为Integer类型
      val minute = StringUtils.convertStringtoInt(dateTime.split(" ")(1).split(":")(1))
      if (minute + (5 - minute % 5) == 60) {
        date + " " + hour + ":" + StringUtils.fulfuill((minute - (minute % 5)) + "") + "~" + date + " " + StringUtils.fulfuill((Integer.parseInt(hour) + 1) + "") + ":00"
      }
      date + " " + hour + ":" + StringUtils.fulfuill((minute - (minute % 5)) + "") + "~" + date + " " + hour + ":" + StringUtils.fulfuill((minute + (5 - minute % 5)) + "")
    }

}
