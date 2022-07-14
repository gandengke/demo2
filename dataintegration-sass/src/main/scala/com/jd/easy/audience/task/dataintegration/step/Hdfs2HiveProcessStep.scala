package com.jd.easy.audience.task.dataintegration.step

import com.jd.easy.audience.common.constant.{NumberConstant, SplitDataSourceEnum, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.task.commonbean.bean.Hdfs2HiveBean
import com.jd.easy.audience.task.commonbean.contant.{BuffaloCircleTypeEnum, DTSMsgTypeEnum, SplitSourceTypeEnum}
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.DataFrameOps._
import com.jd.easy.audience.task.dataintegration.util.{SparkUtil, TableMetaUtil}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession, functions}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util
import scala.collection.JavaConversions.asScalaBuffer

/**
 * 将hdfs数据落库到hive中
 */
class Hdfs2HiveProcessStep extends StepCommon[Unit] {
  val LOGGER: Logger = LoggerFactory.getLogger(classOf[Hdfs2HiveProcessStep])

  def run(dependencies: util.Map[String, StepCommonBean[_]]): Unit = {
    // 1. 参数解析
    val hdfs2HiveBean = getStepBean.asInstanceOf[Hdfs2HiveBean]
    val fieldList = hdfs2HiveBean.getFieldList
    val msgType = hdfs2HiveBean.getMsgType
    var sourceHdfsPath = StringConstant.EMPTY

    // 2. 是否通过数据库区分目录（dts是通过数据库区分目录，其他的使用租户id区分）
    if (hdfs2HiveBean.getIsDirDb) {
      sourceHdfsPath = hdfs2HiveBean.getExtendPath + File.separator + hdfs2HiveBean.getDbName
    } else {
      sourceHdfsPath = hdfs2HiveBean.getExtendPath + File.separator + hdfs2HiveBean.getAccountId
    }

    // 3. spark初始化
    val spark = SparkUtil.buildMergeFileSession("Hdfs2HiveProcessStep_" + hdfs2HiveBean.getAccountId)
    val fsConfig: org.apache.hadoop.conf.Configuration = spark.sparkContext.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get(fsConfig)
    var df = spark.emptyDataFrame

    // 4. init schema
    val schema =
      StructType(
        fieldList.split(StringConstant.COMMA).map(fieldName => StructField(fieldName, StringType, true)))

    // 5. 根据任务类型计算周期路径
    var directoryList: List[String] = getdataHdfsDirList(hdfs2HiveBean.getCircleType.getValue, hdfs2HiveBean.getCircleStep)

    // 6. 根据路径加载数据
    directoryList.foreach(dirEle => {
      //遍历应处理的目录
      val TARGET_PATH = sourceHdfsPath + File.separator + dirEle + File.separator
      if (!isExsit(TARGET_PATH, fileSystem)) {
        LOGGER.info(s"Path does not exist: $TARGET_PATH")
      } else {
        LOGGER.info(s"Path exist,target path: $TARGET_PATH")
        //获取hdfs的叶子目录
        val leafDir = TableMetaUtil.listLeafDir(new Path(TARGET_PATH), fileSystem)
        LOGGER.info("direcory leafDir list:{}", leafDir)
        leafDir.foreach(dir => {
          var currentdf = spark.emptyDataFrame
          if (DTSMsgTypeEnum.JSON.getValue.equalsIgnoreCase(msgType)) {
            currentdf = spark.read.json(dir)
          } else if (DTSMsgTypeEnum.DTS.getValue.equalsIgnoreCase(msgType)) {
            //dts的schema不固定
            currentdf = spark.read.json(dir)
          } else if (DTSMsgTypeEnum.CSVSTREAM.getValue.equalsIgnoreCase(msgType)) {
            var optMap = Map("ignoreLeadingWhiteSpace" -> "true") //忽略字段前后空格
            optMap += ("ignoreTrailingWhiteSpace" -> "true")
            optMap += ("delimiter" -> "\u0002")
            optMap += ("header" -> "false") //字段分隔符
            currentdf = spark.read.schema(schema).options(optMap).csv(dir)
          } else {
            throw new JobInterruptedException("unknown msgType", "unknown msgType")
          }
          loadData(currentdf, spark)
        })
      }
    })
  }

  /**
   * @description 写入数据到表中
   * @param [dataDf, sparkSession]
   * @return void
   * @date 2022/1/19 下午2:57
   * @auther cdxiongmei
   */
  def loadData(dataDf: Dataset[Row], sparkSession: SparkSession): Unit = {
    val hdfs2HiveBean = getStepBean.asInstanceOf[Hdfs2HiveBean]
    val msgType = hdfs2HiveBean.getMsgType
    if (dataDf.isEmpty) {
      return
    }
    dataDf.cache()
    dataDf.show(NumberConstant.INT_10)
    val validStr = dataDf.columns.map("`" + _ + "` is not null").mkString(" or ")
    LOGGER.info("validStr:{}", validStr)
    var validDataDF = dataDf.filter(validStr)
    validDataDF.cache()
    if (NumberConstant.INT_0 == validDataDF.count()) return
    validDataDF.printSchema()
    validDataDF.show(NumberConstant.INT_10)
    // 可能需要从数据中读取表名
    var tableList: util.List[String] = new util.ArrayList[String]()
    if (StringUtils.isNotBlank(hdfs2HiveBean.getTableName)) {
      tableList.add(hdfs2HiveBean.getDbName + StringConstant.DOTMARK + hdfs2HiveBean.getTableName)
      validDataDF = validDataDF.withColumn(ConfigProperties.DTS_FIELD_TABLENAME, functions.lit(hdfs2HiveBean.getDbName + StringConstant.DOTMARK + hdfs2HiveBean.getTableName))
    } else if (DTSMsgTypeEnum.DTS.getValue.equalsIgnoreCase(msgType)) {
      tableList = validDataDF.map(row => row.getAs[String](ConfigProperties.DTS_FIELD_TABLENAME))(Encoders.STRING).distinct().collectAsList()
    }
    LOGGER.info("tablename list=>{}", tableList.toList.toString())
    tableList.foreach(tbName => {
      val subValidDf = validDataDF.filter("`" + ConfigProperties.DTS_FIELD_TABLENAME + "` = \"" + tbName + "\"")
      subValidDf.show(NumberConstant.INT_10)
      if (!sparkSession.catalog.tableExists(tbName) && DTSMsgTypeEnum.DTS.getValue.equalsIgnoreCase(hdfs2HiveBean.getMsgType)) {
        LOGGER.error(s"Error: the table ${tbName} is not exist!")
        return
      } else if (!sparkSession.catalog.tableExists(tbName)) {
        val moduleSql = ConfigProperties.getCreateTableModule(hdfs2HiveBean.getDbName, hdfs2HiveBean.getTableName)
        SparkUtil.printAndExecuteSql(moduleSql, sparkSession)
        TableMetaUtil.writeTableToMysql(sparkSession, "趣云回流数据", SplitSourceTypeEnum.OFFLINETASK, SplitDataSourceEnum.ACCOUNTID, -1L, hdfs2HiveBean.getAccountId, hdfs2HiveBean.getAccountName, hdfs2HiveBean.getDbName, tbName)
      }
      LOGGER.info(s"insert data to tables: " + tbName)
      try {
        subValidDf.saveToHive(tbName, hdfs2HiveBean.getIsHump)
      } catch {
        case e: Exception =>
          sys.exit(-1)
          throw new RuntimeException("Exception exist!")
      }
      LOGGER.info("success tableName={}, datacount={}", tbName, subValidDf.count())
    })
  }

  /**
   * @description 判断hdfs目录是否存在
   * @param [desPath, fileSystem]
   * @return boolean
   * @date 2022/1/19 下午2:58
   * @auther cdxiongmei
   */
  private def isExsit(desPath: String, fileSystem: FileSystem): Boolean = {
    fileSystem.exists(new Path(desPath))
  }

  /**
   * @description 根据周期类型和周期步长获取hdfs路径
   * @param [circleType, circleStep]
   * @return scala.collection.immutable.List<java.lang.String>
   * @date 2022/1/10 下午1:51
   * @auther cdxiongmei
   */
  private def getdataHdfsDirList(circleType: String, circleStep: Integer): List[String] = {
    var directoryList: List[String] = List()

    // 1. 离线任务时间计划执行时间
    val buffaloNtime = System.getenv(StringConstant.BUFFALO_ENV_NTIME)
    LOGGER.info(s"buffaloNtime: $buffaloNtime")
    val jobTime: DateTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern(StringConstant.DATEFORMAT_YYYYMMDDHHMMSS))

    // 2.不同的周期类型，路径加工形式不一样
    if (BuffaloCircleTypeEnum.HOUR.getValue.equalsIgnoreCase(circleType)) {
      // 按照每小时加工
      if (null == circleStep || NumberConstant.INT_24 % circleStep != NumberConstant.INT_0) {
        throw new JobInterruptedException("执行周期步长异常circleType=" + circleType + ",circleStep=" + circleStep)
      }
      for (daysEle <- NumberConstant.INT_1 to circleStep) {
        val curDateTime: DateTime = jobTime.minusHours(daysEle.toInt)
        directoryList = directoryList :+ (curDateTime.toString(ConfigProperties.DATEFORMATE_YYYMMDD_HH))
      }
    } else if (BuffaloCircleTypeEnum.MINUTE.getValue.equalsIgnoreCase(circleType)) {
      // 计算相差的分钟数来得到执行周期
      for (daysEle <- NumberConstant.INT_1 to circleStep) {
        val curDateTime: DateTime = jobTime.minusMinutes(daysEle)
        directoryList = directoryList :+ (curDateTime.toString(ConfigProperties.DATEFORMATE_YYYMMDD_HH_MM))
      }
    } else if (BuffaloCircleTypeEnum.DAY.getValue.equalsIgnoreCase(circleType)) {
      // 按照天加工
      for (daysEle <- NumberConstant.INT_1 to circleStep) {
        val curDateTime: DateTime = jobTime.minusDays(daysEle)
        directoryList = directoryList :+ (curDateTime.toString(ConfigProperties.DATEFORMATE_YYYMMDD))
      }
    } else {
      LOGGER.error("不支持时，分， 天周期之外的任务类型")
      throw new JobInterruptedException("任务周期类型异常")
    }

    LOGGER.info("path directory list:{}", directoryList)
    directoryList
  }
}
