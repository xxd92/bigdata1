package Traffic.bjsxt.util

import java.math.BigDecimal;
/**
  * 数字格式化工具类
  */
object NumberUtils {

  def formatDouble(num:Double, scale:Int) = {
    val bd = new BigDecimal(num)
    //用于格式化小数点，采用四舍五入
    bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue()
  }
}
