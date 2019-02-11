package Traffic.bjsxt.conf

import java.io.InputStream
import java.util.Properties


/**
  * 配置管理组件
  *
  * 1、配置管理组件可以复杂，也可以很简单，对于简单的配置管理组件来说，只要开发一个类，可以在第一次访问它的
  * 		时候，就从对应的properties文件中，读取配置项，并提供外界获取某个配置key对应的value的方法
  * 2、如果是特别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，比如单例模式、解释器模式
  * 		可能需要管理多个不同的properties，甚至是xml类型的配置文件
  * 我们这里的话，就是开发一个简单的配置管理组件，就可以了
  *
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 09:15:21
  */
object ConfigurationManager {
  /**
    *  Properties对象使用private来修饰，就代表了其是类私有的
    *	那么外界的代码，就不能直接通过ConfigurationManager.prop这种方式获取到Properties对象
    *	之所以这么做，是为了避免外界的代码不小心错误的更新了Properties中某个key对应的value
    *	从而导致整个程序的状态错误，乃至崩溃
    */
  val prop:Properties = new Properties()
  try{
    val in:InputStream = ConfigurationManager.getClass.getClassLoader.getResourceAsStream("my.properties")

    // 调用Properties的load()方法，给它传入一个文件的InputStream输入流
    // 即可将文件中的符合“key=value”格式的配置项，都加载到Properties对象中
    // 加载过后，此时，Properties对象中就有了配置文件中所有的key-value对了
    // 然后外界其实就可以通过Properties对象获取指定key对应的value
    prop.load(in)
  }catch {
    case e:Exception => e.printStackTrace()
  }

  /**
    *获取到配置文件中的key对应的值
    * @param key
    * @return
    */
  def getProperty(key:String):String={
    prop.getProperty(key)
  }

  /**
    *获取整数类型的配置项
    * @param key
    * @return
    */
  def getInteger(key:String):Integer={
    val value:String = getProperty(key)
    try{
      Integer.parseInt(value)
      }catch {
        case e:Exception=>e.printStackTrace()
      }
        0
  }

  /**
    *获取布尔类型的配置项
    * @param key
    * @return
    */
  def getBoolean(key:String):Boolean={
    val value:String = getProperty(key)
    try{
      value.toBoolean
    }catch {
      case e:Exception=>e.printStackTrace()
    }
    false
  }


  /**
    *获取Long类型的配置项
    * @param key
    * @return
    */
  def getLong(key:String):Long={
    val value:String = getProperty(key)
    try{
      value.toLong
    }catch {
      case e:Exception=>e.printStackTrace()
    }
    0L
  }








  def main(args: Array[String]): Unit = {
    //println(in.getClass)
  }

}
