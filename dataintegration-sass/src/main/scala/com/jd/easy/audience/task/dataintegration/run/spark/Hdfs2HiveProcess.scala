package com.jd.easy.audience.task.dataintegration.run.spark

import java.math.BigInteger

import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.DataFrameOps._
import com.jd.easy.audience.task.dataintegration.util.SparkUtil
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

@Deprecated
object Hdfs2HiveProcess {
  val LOGGER: Logger = LoggerFactory.getLogger("Hdfs2HiveProcess")
  val BUFFALO_ENV_NTIME = "BUFFALO_ENV_NTIME" //yyyyMMddHHmmss
  val BUFFALO_ENV_BCYCLE = "BUFFALO_ENV_BCYCLE" //2021-09-16-21-15
  val BUFFALO_ENV_CYCLE_TYPE = "BUFFALO_ENV_CYCLE_TYPE" //minute,hour,day,week
  val BDP_USER = "USER" //bdp_User
  def main(args: Array[String]): Unit = {
    LOGGER.info(s"args: ${args.mkString(", ")}")
    LOGGER.info(s"env: ${System.getenv()}")
    if (args.length != NumberConstant.INT_4) {
      LOGGER.error("参数异常：args length less than 4")
      throw new JobInterruptedException("任务参数异常")
    }
    //1. 参数解析
    val extendPath = args(NumberConstant.INT_0) //hdfs数据扩展路径
    val tableName = args(NumberConstant.INT_1) //目标hive表名
    val fieldList = args(NumberConstant.INT_2) //填写字段名
    val msgType = args(NumberConstant.INT_3) //消息类型 json/csvstream
    //2. 离线任务时间计划执行时间
    val buffaloNtime = System.getenv(BUFFALO_ENV_NTIME)
    val buffaloBcycle = System.getenv(BUFFALO_ENV_BCYCLE)
    val cycleType = System.getenv(BUFFALO_ENV_CYCLE_TYPE) //minute,hour,day
    val bdpUser = System.getenv(BDP_USER)
    val jobTime: DateTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    //3. spark初始化
    val spark = SparkUtil.buildMergeFileSession("Hdfs2HiveProcess_" + extendPath)
    val path = s"""${ConfigProperties.getHdfsPath(bdpUser)}/${extendPath}"""
    val fsConfig: org.apache.hadoop.conf.Configuration = spark.sparkContext.hadoopConfiguration
    var directory = ""
    //4. init schema
    val schema =
      StructType(
        fieldList.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    LOGGER.info(s"buffaloNtime: $buffaloNtime, cycleType= $cycleType, buffaloBcycle=$buffaloBcycle")
    //5. 根据任务类型计算周期路径
    if (StringConstant.BUFFALO_CIRLE_HOUR.equalsIgnoreCase(cycleType)) {
      // 按照每小时加工
      val bizTime: DateTime = DateTime.parse(buffaloBcycle, DateTimeFormat.forPattern("yyyy-MM-dd-HH"))
      LOGGER.info(s"hourofday:${jobTime.getHourOfDay},${bizTime.getHourOfDay}")
      //处理跨夜的情况
      var maxHour: Int = jobTime.getHourOfDay
      if (BigInteger.ZERO.intValue() == maxHour) {
        maxHour = NumberConstant.INT_24
      }
      val cirleHour: Int = maxHour - bizTime.getHourOfDay;
      directory = bizTime.toString("yyyy-MM-dd/" + bizTime.getHourOfDay / cirleHour);
    } else if (StringConstant.BUFFALO_CIRLE_MINUTE.equalsIgnoreCase(cycleType)) {
      // 按照分钟加工
      val bizTime: DateTime = DateTime.parse(buffaloBcycle, DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm"))
      LOGGER.info(s"minteofday:${jobTime.getMinuteOfDay},${bizTime.getMinuteOfDay}")
      //计算相差的分钟数来得到执行周期
      var maxMinute: Int = jobTime.getMinuteOfDay
      if (BigInteger.ZERO.intValue() == maxMinute) {
        maxMinute = NumberConstant.INT_24 * NumberConstant.INT_60
      }
      val cirleMin: Int = maxMinute - bizTime.getMinuteOfDay;
      if (NumberConstant.INT_60 % cirleMin != NumberConstant.INT_0) {
        LOGGER.error("不支持不能被60整除的分钟数周期任务cirleMin=" + cirleMin)
        throw new JobInterruptedException("不支持不能被60整除的分钟数周期任务cirleMin=" + cirleMin)
      }
      directory = bizTime.toString("yyyy-MM-dd/HH/") + bizTime.getMinuteOfHour / cirleMin
    } else {
      LOGGER.error("不支持时，分周期之外的任务类型")
      throw new JobInterruptedException("任务周期类型异常")
    }
    //6. 根据路径加载数据
    val TARGET_PATH = s"$path/$directory/"
    if (!isExsit(TARGET_PATH, fsConfig)) {
      LOGGER.info(s"Path does not exist: $TARGET_PATH")
      return
    }
    LOGGER.info(s"target path: $TARGET_PATH")
    var df = spark.emptyDataFrame
    if ("json".equalsIgnoreCase(msgType)) {
      df = spark.read.schema(schema).json(TARGET_PATH)
    } else if ("csvstream".equalsIgnoreCase(msgType)) {
      var optMap = Map("ignoreLeadingWhiteSpace" -> "true", "ignoreTrailingWhiteSpace" -> "true") //忽略字段前后空格
      optMap += ("delimiter" -> "\u0002")
      optMap += ("header" -> "false") //字段分隔符
      df = spark.read.schema(schema).options(optMap).csv(TARGET_PATH)
    }
    val validStr = df.columns.map(_ + " is not null").mkString(" or ")
    val validDataDF = df.filter(validStr)
    //TODO: 测试验证，上线前删除
    df.cache()
    df.printSchema()

    val count = validDataDF.count()
    if (NumberConstant.INT_0 == count) return
    if (!spark.catalog.tableExists(tableName)) {
      LOGGER.error(s"Error: the table ${tableName} is not exist!")
      return
    }
    LOGGER.info(s"insert data to tables: " + tableName)
    try {
      df.saveToHive(tableName, true)
    } catch {
      case e: Exception =>
        sys.exit(-1)
        throw new RuntimeException("Exception exist!")
    }
    LOGGER.info("success datacount={}", count)
  }

  /**
   * 判断hdfs目录是否存在
   *
   * @param desPath
   * @param conf
   * @return
   */
  private def isExsit(desPath: String, conf: org.apache.hadoop.conf.Configuration): Boolean = {
    val fileSystem = FileSystem.get(conf)
    fileSystem.exists(new Path(desPath))
  }

  /**
   * 获取hdfs目录下子文件和子目录
   *
   * @param desPath
   * @param conf
   * @return
   */
  private def listDir(desPath: String, conf: org.apache.hadoop.conf.Configuration): Array[FileStatus] = {
    val fileSystem = FileSystem.get(conf)
    fileSystem.listStatus(new Path(desPath))
  }
}
