package Traffic.bjsxt.constant

/**
  * 常量接口
  *
  * 接口中声明的成员变量默认都是 public static final 的，必须显示的初始化。因而在常量声明时可以省略这些修饰符。
  * @author Administrator
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 10:19:33
  */
object Constants {
  /**
    * 项目配置相关的常量
    */
   val JDBC_DRIVER = "jdbc.driver"
   val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"
   val JDBC_URL = "jdbc.url"
   val JDBC_USER = "jdbc.user"
   val JDBC_PASSWORD = "jdbc.password"
   val JDBC_URL_PROD = "jdbc.url.prod"
   val JDBC_USER_PROD = "jdbc.user.prod"
   val JDBC_PASSWORD_PROD = "jdbc.password.prod"
   val SPARK_LOCAL = "spark.local"
   val SPARK_LOCAL_TASKID_MONITOR = "spark.local.taskId.monitorFlow"
   val SPARK_LOCAL_TASKID_EXTRACT_CAR = "spark.local.taskId.extractCar"
   val SPARK_LOCAL_WITH_THE_CAR = "spark.local.taskId.withTheCar"
   val SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW = "spark.local.taskid.tpn.road.flow"
   val SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT = "spark.local.taskid.road.one.step.convert"
   val KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list"
   val KAFKA_TOPICS = "kafka.topics"

  /**
    * Spark作业相关的常量
    */
  val SPARK_APP_NAME_SESSION = "MonitorFlowAnalyze"
  val FIELD_CAMERA_COUNT = "cameraCount"
  val FIELD_CAMERA_IDS = "cameraIds"
  val FIELD_CAR_COUNT = "carCount"
  val FIELD_NORMAL_MONITOR_COUNT = "normalMonitorCount"
  val FIELD_NORMAL_CAMERA_COUNT = "normalCameraCount"
  val FIELD_ABNORMAL_MONITOR_COUNT = "abnormalMonitorCount"
  val FIELD_ABNORMAL_CAMERA_COUNT = "abnormalCameraCount"
  val FIELD_ABNORMAL_MONITOR_CAMERA_INFOS = "abnormalMonitorCameraInfos"
  val FIELD_TOP_NUM = "topNum"
  val FIELD_DATE_HOUR = "dateHour"
  val FIELD_CAR_TRACK = "carTrack"
  val FIELD_DATE = "dateHour"
  val FIELD_CAR = "car"
  val FIELD_CARS = "cars"
  val FIELD_MONITOR = "monitor"
  val FIELD_MONITOR_ID = "monitorId"
  val FIELD_ACTION_TIME = "actionTime"
  val FIELD_EXTRACT_NUM = "extractNum"
  //低速行驶
  val FIELD_SPEED_0_60 = "0_60"
  //正常行驶
  val FIELD_SPEED_60_90 = "60_90"
  //中速行驶
  val FIELD_SPEED_90_120 = "90_120"
  //高速行驶
  val FIELD_SPEED_120_MAX = "120_max"
  val FIELD_AREA_ID = "areaId"
  val FIELD_AREA_NAME = "areaName"


  /**
    * 任务相关的常量
    */
  val PARAM_START_DATE = "startDate"
  val PARAM_END_DATE = "endDate"
  val PARAM_MONITOR_FLOW = "roadFlow"


}
