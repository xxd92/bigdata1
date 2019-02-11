package util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2018-12-20 17:54:01
  */
object SparkContextUtil {
    def getSparkContextOnLocal(appName:String,master:String) = {
        val sparkConf:SparkConf = new SparkConf() //创建一个SparkConf对象
        /**方式一:在系统环境变量中增加HADOOP_USER_NAME，其值为root;或者 通过java程序动态添加，如下： System.setProperty("HADOOP_USER_NAME", "root");
          *方式二：即开放hadoop中的HDFS目录的权限，命令如下：hadoop fs -chmod -R 777 /
          * 方式三：修改hadoop的配置文件：conf/hdfs-core.xml，添加或者修改 dfs.permissions 的值为 false。
          * 方式四：将IDEA所在机器的名称修改为root，即与服务器上运行hadoop的名称一致。
          */
        System.setProperty("HADOOP_USER_NAME","root")
        /**
          * setMaster主要是连接主节点，如果参数是"local"，则在本地用单线程运行spark;
          * 如果是 local[4]，则在本地用4核运行，如果设置为spark://master:7077，就是作为单节点运行
          * 而setAppName就是在web端显示应用名而已，它们说到底都调用了set（）函数，让我们看看set（）是何方神圣
          * logDeprecation（key）是日志输出函数，防止输入参数名无效， 看看settings，是个HashMap结构
          */
        sparkConf.setMaster(master)
        sparkConf.setAppName(appName)
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN") //设置日志级别，这里为最低的警告级别
        sc
    }

    def getSparkContextOnHDFSCluster(appName:String)={
        val sparkConf:SparkConf = new SparkConf()
        System.setProperty("HADOOP_USER_NAME","root")
        sparkConf
          /**
            *spark.jars:打包程序后，jar包在windows上的文件位置
            *spark.app.name：spark程序在运行时的名字
            *spark.master：master的访问路径
            *spark.driver.host：这个地方要填上你Windows本地的IP地址。
            */
          .set("spark.jars","C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata\\target\\bigdata-all.jar")
          .set("spark.app.name",appName)
          .set("spark.master","spark://ma3:7077")
          //.set("XXD","root")
          .set("spark.driver.host","10.30.161.62")
         // .set("spark.driver.host"," 10.30.161.62")
          new SparkContext(sparkConf)
    }
}
