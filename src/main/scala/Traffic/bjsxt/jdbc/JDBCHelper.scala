package Traffic.bjsxt.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import Traffic.bjsxt.conf.ConfigurationManager
import Traffic.bjsxt.constant.Constants
import java.util

import Traffic.bjsxt.jdbc.JDBCHelper.datasource
/**
  * JDBC辅助组件
  *
  * 在正式的项目的代码编写过程中，是完全严格按照大公司的coding标准来的
  * 也就是说，在代码中，是不能出现任何hard code（硬编码）的字符
  * 比如“张三”、“com.mysql.jdbc.Driver”
  * 所有这些东西，都需要通过常量来封装和使用
  *
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 09:08:57
  */
object JDBCHelper {
  //加载数据库的驱动
  try {
    val driver: String = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
    Class.forName(driver)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  // 第二步，实现JDBCHelper的单例化
  // 为什么要实现单例化呢？因为它的内部要封装一个简单的内部的数据库连接池
  // 为了保证数据库连接池有且仅有一份，所以就通过单例的方式
  // 保证JDBCHelper只有一个实例，实例中只有一份数据库连接池

  val instance: JDBCHelper = null

  /**
    * 获取单例
    *
    * @return
    */
  def getInstance: JDBCHelper = {
    new JDBCHelper
  }

  private val datasource: util.LinkedList[Connection] = new util.LinkedList[Connection]() //连接池


  object JDBCHelper {
    //获取连接池大小
    val datasourceSize: Int = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE)
    //创建指定数量的数据库连接，并放入数据库连接池中
    for (i <- 0 until datasourceSize) {
      val local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
      var url: String = null
      var user: String = null
      var password: String = null
      if (local) {
        url = ConfigurationManager.getProperty(Constants.JDBC_URL)
        user = ConfigurationManager.getProperty(Constants.JDBC_USER)
        password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
      } else {
        url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
        user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
        password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
      }

      try {
        val conn: Connection = DriverManager.getConnection(url, user, password)
        datasource.push(conn)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }


  //提供获取数据库连接的方法
  def getConnection: Connection = {
    //同步代码块
    AnyRef.synchronized({
      while (datasource.isEmpty) {
        try {
          println("Jdbc datasource has no connection now, please wait a moments!")
          Thread.sleep(2000)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
      datasource.poll()
    })
  }


  /**
    * 第五步：开发增删改查的方法
    * 1、执行增删改SQL语句的方法
    * 2、执行查询SQL语句的方法
    * 3、批量执行SQL语句的方法
    *
    * @param sql
    * @param params
    * @return
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn: Int = 0
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      conn = getConnection
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null)
        datasource.push(conn)
    }
    rtn
  }

  /**
    * 执行查询SQL语句
    *
    * @param sql
    * @param params
    * @param callback
    */
  def executeQuery(sql: String, params: Array[Any], callback: QueryCallback) = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = getConnection
      pstmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rs = pstmt.executeQuery()
      callback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) datasource.push(conn)
    }
  }

  def executeBatch(sql: String, paramsList: List[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      // 第一步：使用Connection对象，取消自动提交
      conn = getConnection
      pstmt = conn.prepareStatement(sql)
      //第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
      if (paramsList != null && paramsList.size > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      //第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
      rtn = pstmt.executeBatch()
      // 最后一步：使用Connection对象，提交批量的SQL语句
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) datasource.push(conn)
    }
    rtn
  }

  /**
    * 静态内部类：查询回调接口
    */
  trait QueryCallback {
    /**
      * 处理查询结果
      *
      * @param rs
      * @return
      */
    def process(rs: ResultSet) = {
      new Exception
    }
  }
}
case class JDBCHelper()
