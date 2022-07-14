package com.jd.easy.audience.task.dataintegration.step

import java.io.File
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.{Date, UUID}

import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.OssFileTypeEnum
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.Oss2HiveBean
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.TableMetaUtil.getModificationTime
import com.jd.easy.audience.task.dataintegration.util.{SparkUtil, TableMetaInfo, TableMetaUtil}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.jss.JingdongStorageService
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer

/**
 * 将特定oss目录下的数据导入到指定的hive表中
 */
class OssToHiveStep extends StepCommon[util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[OssToHiveStep])

  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: util.Map[String, StepCommonBean[_]]): util.Map[String, AnyRef] = {
    //1. 初始化spark配置
    val bean: Oss2HiveBean = getStepBean.asInstanceOf[Oss2HiveBean]
    val dbName: String = bean.getDbName
    val targetTable: String = bean.getTargetTable.trim.toLowerCase()
    val sparkSession = SparkUtil.buildMergeFileSession(bean.getStepName);
    //2. 元数据信息的前置校验
    if (!sparkSession.catalog.databaseExists(dbName)) {
      throw new JobInterruptedException(s"""$dbName 库不存在，请重新检查""", s"""Failure: The database $dbName is not exist!""")
    }
    if (!sparkSession.catalog.tableExists(s"""$dbName.$targetTable""")) {
      throw new JobInterruptedException(s"""$dbName.$targetTable 表不存在，请重新检查""", s"""Failure: The table $dbName.$targetTable is not exist!""")
    }
    loadOssData(sparkSession, bean)

    //8. 查看元数据信息
    val fileInfo: TableMetaInfo = TableMetaUtil.getTargetTableMetaInfo(dbName, targetTable, sparkSession)

    //9. 返回值处理
    var resultMap: util.HashMap[String, Object] = new util.HashMap[String, Object]()
    resultMap.put("fileSize", fileInfo.getfileSizeFormat())
    resultMap.put("rowNumber", fileInfo.getrowNumber().toString)
    resultMap.put("modifiedtime", fileInfo.getmodificationTimeFormat())
    resultMap.put("partitionKey", StringUtils.join(fileInfo.getPartitions(), StringConstant.COMMA))
    LOGGER.info("resultMap=" + resultMap.toString)
    bean.setOutputMap(resultMap)
    resultMap

  }

  /**
   * 从hdfs上的文件读取数据内容
   *
   * @param sparkSession sparksession
   * @param config       导数的配置
   * @param fileName     hdfs上的文件名
   * @param fileType     数据文件类型
   * @return
   */
  def file2Dataset(sparkSession: SparkSession, config: Map[String, String], fileName: String, fileType: OssFileTypeEnum): Dataset[Row] = {
    LOGGER.info("file2Dataset fileName=" + fileName)
    var dataset: Dataset[Row] = sparkSession.emptyDataFrame
    try {
      if ("csv".equalsIgnoreCase(fileType.getValue)) {
        dataset = sparkSession.read.options(config).csv(fileName);
      } else if ("json".equalsIgnoreCase(fileType.getValue)) {
        dataset = sparkSession.read.options(config).json(fileName).repartition(NumberConstant.INT_1);
      } else if ("txt".equalsIgnoreCase(fileType.getValue)) {
        dataset = sparkSession.read.options(config).text(fileName).repartition(NumberConstant.INT_1);
      } else {
        throw new JobInterruptedException("暂时不能识别的文件类型", "Exception: The fileType " + fileType + " cannot be identified!")
      }
    } catch {
      case e: Exception =>
        if (e.isInstanceOf[JobInterruptedException]) {
          throw e
        }
        throw new JobInterruptedException("数据行出现异常", "Some data contains incorrect char")
    }
    return dataset
  }

  /**
   * jssclient下载文件
   *
   * @param sparkSession
   * @param bean
   * @return
   */
  def ossDir2hdfsDir(sparkSession: SparkSession, bean: Oss2HiveBean): java.util.List[String] = {
    val hdfsPath = "tmpcsv/" + sparkSession.sparkContext.applicationId + File.separator
    val configuration = sparkSession.sparkContext.hadoopConfiguration
    val jfsClient: JingdongStorageService = JdCloudOssUtil.createJfsClient(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
    val fileList = JdCloudOssUtil.copyDirectoryToHdfs(jfsClient, configuration, bean.getBucket, bean.getFilePath, hdfsPath);
    fileList
  }

  /**
   * 获取oss数据并导入到hive表中
   *
   * @param sparkSession
   * @param bean
   * @return
   */
  def loadOssData(sparkSession: SparkSession, bean: Oss2HiveBean): Unit = {
    val fileType: OssFileTypeEnum = bean.getFileType
    val targetDbTable: String = bean.getDbName + StringConstant.DOTMARK + bean.getTargetTable
    //元信息获取
    val metaInfo = sparkSession.catalog.listColumns(targetDbTable)
    metaInfo.show
    val colCount = metaInfo.count()
    //3. schema信息
    val fieldNames = metaInfo.map(element => element.name)(Encoders.STRING).collect().mkString(StringConstant.COMMA)

    //4. 分区字段过滤(可能存在多个分区字段)
    var partitionColsList: java.util.List[String] = new java.util.ArrayList[String]()
    metaInfo.filter(ConfigProperties.FILTER_PATITION_FIELD).collectAsList().foreach(partitionCol => {
      partitionColsList.add(partitionCol.name)
    })
    //6. jssclient下载文件
    val fileList: java.util.List[String] = ossDir2hdfsDir(sparkSession, bean)
    LOGGER.info("fieldNames:" + fieldNames + "待导入的文件数:{}" + fileList.size())
    var circle: Int = NumberConstant.INT_1
    //5. 数据格式限制
    var optMap = Map("ignoreLeadingWhiteSpace" -> "true") //忽略字段前后空格
    optMap += ("ignoreTrailingWhiteSpace" -> "true")
    optMap += ("header" -> "true"); //是否包含表头
    optMap += ("delimiter" -> ","); //字段分隔符
    optMap += ("encoding" -> bean.getEncoding); //编码格式
    optMap += ("quote" -> "\'"); //包裹分隔符
    optMap += ("escape" -> "\\"); //解析包裹分隔符的标记
    optMap += ("mode" -> "FAILFAST"); //遇到错误就返回
    optMap += ("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"); //timestamp字段格式
    optMap += ("dateFormat" -> "yyyy-MM-dd") //date字段格式
    optMap += ("maxColumns" -> s"""$colCount""") //最大列数
    optMap += ("inferSchema" -> "false") //需要从csv数据读表格式

    fileList.foreach(fileEle => {
      LOGGER.info("load data ...fileName={}", bean.getFilePath)
      val dataset = file2Dataset(sparkSession, optMap, fileEle, fileType)
      //8. 将数据缓存到内存
      dataset.cache()
      dataset.printSchema()
      if (dataset.columns.size != colCount) {
        LOGGER.info("csv columns count=" + dataset.columns.size + ", schema columns count= " + colCount)
        throw new JobInterruptedException("提供的数据和元数据列数不一致", "Failure: The columns of file and metadata are differ!")
      } else if (NumberConstant.INT_0 == dataset.count()) {
        LOGGER.info(fileEle + "导入的数据为0")
      }

      //10. 判断是否是分区表
      if (partitionColsList.nonEmpty) {
        writePartitionData(partitionColsList, sparkSession, dataset, bean.getOverWrite, fieldNames, targetDbTable)
      } else {
        val tmpViewNameAll = "dataset_upload_view"
        dataset.createOrReplaceTempView(tmpViewNameAll)
        //10.2 没有分区字段
        var isOverWrite: Boolean = false
        if (bean.getOverWrite && circle == NumberConstant.INT_1) {
          isOverWrite = true
        }
        val tableInfo: LoadTableInfo = new LoadTableInfo(isOverWrite, targetDbTable, tmpViewNameAll, fieldNames, partitionColsList)
        dataToHive(sparkSession, tableInfo)
        val curDataCnt: Long = dataset.count()
        LOGGER.info("本次导入的数据为：" + curDataCnt)
      }
    })
  }

  /**
   * @description 分区表按照分区分批写入
   * @param [partitionColsList, sparkSession, dataset, isOverWrite, fieldNames, targetTable]
   * @return void
   * @date 2022/1/19 上午10:01
   * @auther cdxiongmei
   */
  def writePartitionData(partitionColsList: java.util.List[String], sparkSession: SparkSession, dataset: Dataset[Row], isOverWrite: Boolean, fieldNames: String, targetTable: String): Unit = {
    //10.1 分区表
    val tmpViewNameAll = "dataset_upload_view"
    val datasetNew = dataset.selectExpr("*", s"""concat_ws("par",${partitionColsList.mkString(StringConstant.COMMA)}) as partion_info""")
    datasetNew.createOrReplaceTempView(tmpViewNameAll)
    var partitionData: Map[String, Long] = Map[String, Long]()
    //如果有分区字段
//    var selectFieldStr: scala.collection.immutable.List[String] = scala.collection.immutable.List[String]()
//    partitionColsList.foreach(partition => {
//      selectFieldStr = selectFieldStr :+ "cast(" + partition + " as STRING) AS " + partition
//    })
    val partitionsValues = datasetNew.select("partion_info").distinct().as(Encoders.STRING).collect()
//    val partitionsValues = sparkSession.sql(
//      s"""
//         |SELECT concat_ws("par",${partitionColsList.mkString(StringConstant.COMMA)}) as partion_info
//         |FROM $tmpViewNameAll
//         |GROUP BY ${partitionColsList.mkString(StringConstant.COMMA)}
//         |""".stripMargin).select("partion_info").as(Encoders.STRING).collect()

    partitionsValues.foreach(dtEle => {
//      var dtValue = dtEle.getAs[String](NumberConstant.INT_0)
//      var filterString = s"""${partitionColsList(NumberConstant.INT_0)}='$dtValue'"""
//      if (partitionColsList.size == NumberConstant.INT_2) {
//        dtValue = dtValue + dtEle.getAs[String](NumberConstant.INT_1)
//        filterString = filterString + s""" and ${partitionColsList(NumberConstant.INT_1)}='${dtEle.getString(NumberConstant.INT_1)}'"""
//      }
      var datasetInsert = datasetNew.filter("partion_info=\"" + dtEle + "\"")
      val curDataCnt: Long = datasetInsert.count()
      var isOverWriteCur: Boolean = false
      if (isOverWrite && !partitionData.contains(dtEle)) {
        isOverWriteCur = true
        partitionData += (dtEle -> curDataCnt)
      } else {
        val count: Long = partitionData.getOrElse(dtEle, NumberConstant.LONG_0) + curDataCnt
        partitionData += (dtEle -> count)
      }
      val tmpViewName = UUID.randomUUID.toString.replaceAll(StringConstant.MIDDLELINE, StringConstant.EMPTY) + StringConstant.UNDERLINE + "subView"
      datasetInsert.createOrReplaceTempView(tmpViewName)
      val tableInfo: LoadTableInfo = new LoadTableInfo(isOverWriteCur, targetTable, tmpViewName, fieldNames, partitionColsList)
      dataToHive(sparkSession, tableInfo)
    })
    LOGGER.info("分区为：" + partitionData + "完成," + "本次导入的数据为：" + partitionData.values.sum)
  }

  /**
   * @description 将临时视图数据保存到hive表
   * @param [sparkSession, tableInfo]
   * @return void
   * @date 2022/1/19 上午10:33
   * @auther cdxiongmei
   */
  def dataToHive(sparkSession: SparkSession, tableInfo: LoadTableInfo): Unit = {
    var saveModeKey = "INTO"
    if (tableInfo.isOverWrite()) {
      saveModeKey = "OVERWRITE"
    }
    var partitionInfo = StringConstant.EMPTY
    if (CollectionUtils.isNotEmpty(tableInfo.getPartitions())) {
      partitionInfo = s"""PARTITION(${tableInfo.getPartitions().mkString(",")})"""
    }
    SparkUtil.printAndExecuteSql(
      s"""
         |INSERT ${saveModeKey} TABLE ${tableInfo.getTableName()} ${partitionInfo}
         |SELECT
         |    ${tableInfo.getFieldNames()}
         |FROM
         |    ${tableInfo.getSourceTableName()}
         |""".stripMargin, sparkSession)

  }

  class LoadTableInfo(isOverWrite: Boolean, tableName: String, sourceTableName: String, fieldNames: String, partitions: java.util.List[String]) {

    def getTableName(): String = {
      tableName
    }

    def getSourceTableName(): String = {
      sourceTableName
    }

    def getFieldNames(): String = {
      fieldNames
    }

    def isOverWrite(): Boolean = {
      isOverWrite
    }

    def getPartitions(): java.util.List[String] = {
      partitions
    }

    override def toString: String = "{tableName=" + tableName + ",sourceTableName=" + sourceTableName + ",fieldNames=" + fieldNames + ", isOverWrite=" + isOverWrite + ", partitions=" + StringUtils.join(partitions, StringConstant.COMMA) + "}"
  }

}