package com.jd.easy.audience.task.plugin.step.dataset.udd

import java.io.{File, IOException}
import java.util.HashMap

import com.alibaba.fastjson.JSONObject
import com.jd.easy.audience.common.constant.{LogicalOperatorEnum, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.UddChainFlowBean
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.step.dataset.udd.util.DateUtil
import com.jd.easy.audience.task.plugin.util.es.EsUtil
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster
import com.jd.easy.audience.task.plugin.util.es.sqltool.{ComparisonOperatorsEnum, SqlCondition}
import com.jd.jss.JingdongStorageService
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.codehaus.jackson.annotate.JsonIgnore
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.collection.mutable.Map

/**
 * 4a 流转数据
 */
class UddChainFlowGeneratorStep extends StepCommon[java.util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[UddChainFlowGeneratorStep])
  @JsonIgnore
  private val statusResultColumns = Seq(
    "user_id",
    "status",
    "a1_feature_map",
    "a2_feature_map",
    "a3_feature_map",
    "a4_feature_map",
    "dt",
    "dataset_id"
  )
  override def validate(): Unit = {
    val bean: UddChainFlowBean = getStepBean.asInstanceOf[UddChainFlowBean];

    if (StringUtils.isBlank(bean.getEndPoint) || StringUtils.isBlank(bean.getBucket) || StringUtils.isBlank(bean.getAccessKey) || StringUtils.isBlank(bean.getSecretKey) || StringUtils.isBlank(bean.getOssFilePath)) {
      throw new JobInterruptedException("uddChainFlowGeneratorStep oss配置参数错误", "Error: The Oss_Configuration of uddChainFlowGeneratorStep Error");
    } else if (StringUtils.isBlank(bean.getEsNode) || StringUtils.isBlank(bean.getEsIndex)) {
      throw new JobInterruptedException("uddChainFlowGeneratorStep es配置参数错误", "Error: The ES_Configuration of uddChainFlowGeneratorStep Error");
    } else if (StringUtils.isBlank(bean.getDatasetId)) {
      throw new JobInterruptedException("uddChainFlowGeneratorStep 数据集id参数异常", "Error: The datasetId of uddChainFlowGeneratorStep Error");
    }
  }

  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: java.util.Map[String, StepCommonBean[_]]): java.util.Map[String, AnyRef] = {
    val bean: UddChainFlowBean = getStepBean.asInstanceOf[UddChainFlowBean];
    LOGGER.info("esNode：{}, esPort={}, esUser={}, esPassword={}", bean.getEsNode, bean.getEsPort, bean.getEsUser, bean.getEsPassword)
    deleteEsData(bean)
    val sparkSession = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode, bean.getEsPort, bean.getEsUser, bean.getEsPassword, if(StringUtils.isBlank(bean.getStepName)) "UddChainFlowStep" else bean.getStepName);
    //1. 明细数据获取
    var parquetFilePath = JdCloudOssUtil.getUddStatusDataKey(bean.getOssFilePath, bean.getStatDate);
    val dateStrHis = DateUtil.getDateStrHis(bean.getStatDate, bean.getPeriod)
    var parquetFilePathHis = JdCloudOssUtil.getUddStatusDataKey(bean.getOssFilePath, dateStrHis);
    //明细数据默认数据
    val schema = StructType(statusResultColumns.map(fieldName => {
      if (fieldName.equalsIgnoreCase("user_id") || fieldName.equalsIgnoreCase("dt") || fieldName.equalsIgnoreCase("dataset_id")) {
        StructField(fieldName, DataTypes.StringType, true)
      } else if (fieldName.equalsIgnoreCase("status")) {
        StructField(fieldName, DataTypes.IntegerType, true)
      } else {
        StructField(fieldName, DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.DoubleType)), true)
      }

    }))
    //主要是利用了spark.sparkContext.emptyRDD
    val jfsClient: JingdongStorageService = JdCloudOssUtil.createJfsClient(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
    var statusDF: Dataset[Row] = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    var statusHisDF: Dataset[Row] = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    if (!jfsClient.hasBucket(bean.getBucket)) {
      throw new JobInterruptedException("bucket不存在bucketname=" + bean.getBucket, s"""Failure: The bucket ${bean.getBucket} is not exist!""")
    }
    if (JdCloudOssUtil.objectExist(jfsClient, bean.getBucket, parquetFilePath)) {
      //数据存在
      val path:String = bean.getBucket + File.separator + parquetFilePath
      val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
      readconfig.setOptions(new HashMap[String, String]() {{put("header", "true")}})
      readconfig.setClientType(ConfigProperties.getClientType)
      readconfig.setFileType(OssFileTypeEnum.parquet)
      readconfig.setObjectKey(path)
      statusDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, readconfig)

//      statusDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, bean.getEndPoint, bean.getAccessKey, bean.getSecretKey, path, OssFileTypeEnum.parquet, ConfigProperties.getClientType)
    }
    if (JdCloudOssUtil.objectExist(jfsClient, bean.getBucket, parquetFilePathHis)) {
      //数据存在
      val pathHis:String = bean.getBucket + File.separator + parquetFilePathHis

      val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
      readconfig.setOptions(new HashMap[String, String]() {{put("header", "true")}})
      readconfig.setClientType(ConfigProperties.getClientType)
      readconfig.setFileType(OssFileTypeEnum.parquet)
      readconfig.setObjectKey(pathHis)
      statusHisDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, readconfig)

//      statusHisDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, bean.getEndPoint, bean.getAccessKey, bean.getSecretKey, pathHis, OssFileTypeEnum.parquet, ConfigProperties.getClientType)
    }
    val chainFlowDf = getChainFlowDf(statusDF, statusHisDF, sparkSession)
    val formatChainFlowDf = getFormatChainFlowDf(chainFlowDf, bean)
    chainFlowDfToSave(formatChainFlowDf, 1, bean.getEsIndex)
    //2. 元数据信息返回
    val resultMap = new java.util.HashMap[String, Object]()
    LOGGER.info("resultMap=" + resultMap.toString)
    resultMap
  }

  @JsonIgnore
  private val sumConsumerStr: String =
    s"""
       |map(
       |  "a0toa1",sum(case when start_status IS NULL and end_status=1 then 1 else 0 end),
       |  "a0toa2",sum(case when start_status IS NULL and end_status=2 then 1 else 0 end),
       |  "a0toa3",sum(case when start_status IS NULL and end_status=3 then 1 else 0 end),
       |  "a0toa4",sum(case when start_status IS NULL and end_status=4 then 1 else 0 end),
       |  "a1toa5",sum(case when start_status=1 and end_status IS NULL then 1 else 0 end),
       |  "a1toa1",sum(case when start_status=1 and end_status=1 then 1 else 0 end),
       |  "a1toa2",sum(case when start_status=1 and end_status=2 then 1 else 0 end),
       |  "a1toa3",sum(case when start_status=1 and end_status=3 then 1 else 0 end),
       |  "a1toa4",sum(case when start_status=1 and end_status=4 then 1 else 0 end),
       |  "a2toa5",sum(case when start_status=2 and end_status IS NULL then 1 else 0 end),
       |  "a2toa1",sum(case when start_status=2 and end_status=1 then 1 else 0 end),
       |  "a2toa2",sum(case when start_status=2 and end_status=2 then 1 else 0 end),
       |  "a2toa3",sum(case when start_status=2 and end_status=3 then 1 else 0 end),
       |  "a2toa4",sum(case when start_status=2 and end_status=4 then 1 else 0 end),
       |  "a3toa5",sum(case when start_status=3 and end_status IS NULL then 1 else 0 end),
       |  "a3toa1",sum(case when start_status=3 and end_status=1 then 1 else 0 end),
       |  "a3toa2",sum(case when start_status=3 and end_status=2 then 1 else 0 end),
       |  "a3toa3",sum(case when start_status=3 and end_status=3 then 1 else 0 end),
       |  "a3toa4",sum(case when start_status=3 and end_status=4 then 1 else 0 end),
       |  "a4toa5",sum(case when start_status=4 and end_status IS NULL then 1 else 0 end),
       |  "a4toa1",sum(case when start_status=4 and end_status=1 then 1 else 0 end),
       |  "a4toa2",sum(case when start_status=4 and end_status=2 then 1 else 0 end),
       |  "a4toa3",sum(case when start_status=4 and end_status=3 then 1 else 0 end),
       |  "a4toa4",sum(case when start_status=4 and end_status=4 then 1 else 0 end)
       |) as ax_to_ay_count
     """.stripMargin.replaceAll("\n", " ")

  private def getChainFlowDf(statusDF: Dataset[Row], statusDFHis: Dataset[Row], sparkSession: SparkSession): DataFrame = {
    val statusViewName = "status_view_table"
    val statusHisViewName = "status_ytd_view_table"

    statusDF.createOrReplaceTempView(statusViewName)
    statusDFHis.createOrReplaceTempView(statusHisViewName)
    val statusSql =
      s"""
         |select
         |  case when t1.dataset_id is not null then t1.dataset_id else t2.dataset_id end AS dataset_id,
         |  $sumConsumerStr
         |from
         |  (
         |    select
         |      dataset_id,
         |      user_id,
         |      status AS end_status
         |    from $statusViewName
         |  ) t1
         |full outer join
         |  (
         |    select
         |      dataset_id,
         |      user_id,
         |      status AS start_status
         |    from ${statusHisViewName}
         |  ) t2
         |on t1.dataset_id=t2.dataset_id AND t1.user_id=t2.user_id
         |group by case when t1.dataset_id is not null then t1.dataset_id else t2.dataset_id end
       """.stripMargin.replaceAll("\n", " ")
    LOGGER.info("chain flow sql = " + statusSql)
    sparkSession.sql(statusSql)
  }

  private def getFormatChainFlowDf(chainFlowDf: DataFrame, bean: UddChainFlowBean): DataFrame = {
    val dateStrHis = DateUtil.getDateStrHis(bean.getStatDate, bean.getPeriod)
    chainFlowDf.withColumn("dt", lit(bean.getStatDate))

      .withColumn("logic_dt", lit(DateUtil.getLogicDt(dateStrHis, bean.getStatDate, bean.getPeriod)))
      .withColumn("start_date", lit(dateStrHis))
      .withColumn("end_date", lit(bean.getStatDate))
  }

  @JsonIgnore
  private val resultColumns = Seq(
    "dataset_id",
    "ax_to_ay_count",
    "logic_dt",
    "start_date",
    "end_date",
    "dt"
  )

  private def chainFlowDfToSave(dataFrame: DataFrame, numPartsToSave: Int, esIndex: String): Unit = {
    val bean: UddChainFlowBean = getStepBean.asInstanceOf[UddChainFlowBean]
    val dfToWriter = dataFrame.select(resultColumns.head, resultColumns.tail: _*).withColumn("period_desc", lit(bean.getPeriod)).repartition(numPartsToSave)
    dfToWriter.cache()
    LOGGER.info("chainFlowDfToSave chainFlowwrite data to es count:{}", dfToWriter.count())
    val dfToEs: DataFrame = dfToWriter
      .withColumnRenamed("dataset_id", "datasetId")
      .withColumnRenamed("ax_to_ay_count", "axToAyCount")
      .withColumnRenamed("logic_dt", "logicDt")
      .withColumnRenamed("start_date", "startDate")
      .withColumnRenamed("end_date", "endDate")
      .withColumnRenamed("dt", "processDate")
      .withColumn("version", lit(ConfigProperties.VERSION))
      .withColumnRenamed("period_desc", "periodDesc")
      .withColumn("id", concat_ws("-", col("logicDt"), col("datasetId")))
    EsSparkSQL.saveToEs(dfToEs, esIndex, Map("es.mapping.id" -> "id"))
  }
  private def deleteEsData(bean: UddChainFlowBean): Unit = {
    val condDatasetId: SqlCondition = new SqlCondition
    condDatasetId.setColumnName("datasetId")
    condDatasetId.setParam1(bean.getDatasetId.toString)
    condDatasetId.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condDatasetId.setLogicalOperator(LogicalOperatorEnum.and)
    val condDate: SqlCondition = new SqlCondition
    condDate.setColumnName("processDate")
    condDate.setParam1(bean.getStatDate)
    condDate.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condDate.setLogicalOperator(LogicalOperatorEnum.and)
    val condPeriod: SqlCondition = new SqlCondition
    condPeriod.setColumnName("periodDesc")
    condPeriod.setParam1(bean.getPeriod)
    condPeriod.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condPeriod.setLogicalOperator(LogicalOperatorEnum.and)
    val conds: java.util.List[SqlCondition] = new java.util.ArrayList[SqlCondition]() {{
      add(condDatasetId)
      add(condDate)
      add(condPeriod)
    }}
    val dslJson: JSONObject = EsUtil.queryDslJSON(conds)
    val esCluster = new EsCluster
    esCluster.setEsNodes(bean.getEsNode)
    esCluster.setEsPort(bean.getEsPort)
    esCluster.setUsername(bean.getEsUser)
    esCluster.setPassword(bean.getEsPassword)
    esCluster.setEsIndex(bean.getEsIndex)
    LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString)
    EsUtil.deleteEsData(esCluster, dslJson)

  }
//  /**
//   * 任务运行前提前写入空数据
//   *
//   * @param sparkSession
//   */
//  private def writeEmptyDataToEs(sparkSession: SparkSession): Unit = {
//    val dateStrHis = DateUtil.getDateStrHis(statDate, period)
//    val resultSchema = StructType(resultColumns.map(fieldName => {
//      if (fieldName.equalsIgnoreCase("dt") || fieldName.equalsIgnoreCase("end_date") || fieldName.equalsIgnoreCase("start_date")) {
//        StructField(fieldName, DataTypes.StringType, true)
//      } else if (fieldName.equalsIgnoreCase("dataset_id") || fieldName.equalsIgnoreCase("logic_dt")) {
//        StructField(fieldName, DataTypes.LongType, true)
//      } else {
//        StructField(fieldName, DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType), true)
//      }
//    }))
//    var tagsScala: Map[String, Long] = Map()
//    for (i <- 0 to 4) {
//      for (j <- 0 to 5) {
//        val key: String = "a" + i + "toa" + j
//        tagsScala.put(key, -1L)
//      }
//    }
//    tagsScala.remove("a0toa5")
//    println(tagsScala.toString())
//    var data: java.util.List[Row] = Lists.newArrayList(Row(getDatasetId.toLong, tagsScala, DateUtil.getLogicDt(dateStrHis, statDate, period), dateStrHis, statDate, statDate))
//    var emptyData = sparkSession.createDataFrame(data, resultSchema)
//    LOGGER.info("ready to write emptydata to es")
//    emptyData.show(10)
//    chainFlowDfToSave(emptyData, 1)
//  }
}