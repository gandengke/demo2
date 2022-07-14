package com.jd.easy.audience.task.plugin.step.dataset.udd

import java.io.File
import java.util.HashMap

import com.alibaba.fastjson.JSONObject
import com.jd.easy.audience.common.constant.{LogicalOperatorEnum, NumberConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.UddAssetsBean
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

/**
 * 4a分层数据加工
 */
class UddAssetsGeneratorStep extends StepCommon[java.util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[UddAssetsGeneratorStep])
  // tb schema
  @JsonIgnore
  private val resultColumns = Seq(
    "dataset_id",
    "a1_cust_count",
    "a1_cust_inflow",
    "a1_cust_outflow",
    "a2_cust_count",
    "a2_cust_inflow",
    "a2_cust_outflow",
    "a3_cust_count",
    "a3_cust_inflow",
    "a3_cust_outflow",
    "a4_cust_count",
    "a4_cust_inflow",
    "a4_cust_outflow",
    "dt"
  )
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
  @JsonIgnore
  private val sumConsumerStr: String =
    s"""
       |  SUM( CASE WHEN t1.status = 1 THEN 1 ELSE 0 END )AS a1_cust_count,
       |  SUM( CASE WHEN t1.status = 2 THEN 1 ELSE 0 END )AS a2_cust_count,
       |  SUM( CASE WHEN t1.status = 3 THEN 1 ELSE 0 END )AS a3_cust_count,
       |  SUM( CASE WHEN t1.status = 4 THEN 1 ELSE 0 END )AS a4_cust_count,
       |  SUM( CASE WHEN t2.status = 1 AND (t1.status !=1 OR t1.status IS NULL) THEN 1 ELSE 0 END) AS a1_cust_outflow,
       |  SUM( CASE WHEN t2.status = 2 AND (t1.status !=2 OR t1.status IS NULL) THEN 1 ELSE 0 END) AS a2_cust_outflow,
       |  SUM( CASE WHEN t2.status = 3 AND (t1.status !=3 OR t1.status IS NULL) THEN 1 ELSE 0 END) AS a3_cust_outflow,
       |  SUM( CASE WHEN t2.status = 4 AND (t1.status !=4 OR t1.status IS NULL) THEN 1 ELSE 0 END) AS a4_cust_outflow,
       |  SUM( CASE WHEN t1.status = 1 AND (t2.status !=1 OR t2.status IS NULL)  THEN 1 ELSE 0 END) AS a1_cust_inflow,
       |  SUM( CASE WHEN t1.status = 2 AND (t2.status !=2 OR t2.status IS NULL)  THEN 1 ELSE 0 END) AS a2_cust_inflow,
       |  SUM( CASE WHEN t1.status = 3 AND (t2.status !=3 OR t2.status IS NULL) THEN 1 ELSE 0 END) AS a3_cust_inflow,
       |  SUM( CASE WHEN t1.status = 4 AND (t2.status !=4 OR t2.status IS NULL)  THEN 1 ELSE 0 END) AS a4_cust_inflow
     """.stripMargin.replaceAll("\n", " ")

  override def validate(): Unit = {
    val bean: UddAssetsBean = getStepBean.asInstanceOf[UddAssetsBean];

    if (null == bean.getEndPoint || null == bean.getBucket || null == bean.getAccessKey || null == bean.getSecretKey || null == bean.getOssFilePath) {
      throw new JobInterruptedException("uddAssetsGeneratorStep oss配置参数错误", "Error: The Oss_Configuration of uddAssetsGeneratorStep Error");
    } else if (null == bean.getEsNode || null == bean.getEsIndex) {
      throw new JobInterruptedException("uddAssetsGeneratorStep es配置参数错误", "Error: The ES_Configuration of uddAssetsGeneratorStep Error");
    } else if (null == bean.getOssFilePath) {
      throw new JobInterruptedException("uddAssetsGeneratorStep 明细数据路径异常", "Error: The StatusData_Path of uddAssetsGeneratorStep Error");
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
    val bean: UddAssetsBean = getStepBean.asInstanceOf[UddAssetsBean];
    LOGGER.info("esNode：{}, esPort={}, esUser={}, esPassword={}", bean.getEsNode, bean.getEsPort, bean.getEsUser, bean.getEsPassword)
    deleteEsData(bean)
    val sparkSession = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode, bean.getEsPort, bean.getEsUser, bean.getEsPassword, if(StringUtils.isBlank(bean.getStepName)) "UddAssetsStep" else bean.getStepName);
    //1. 明细数据获取
    var parquetFilePath: String = JdCloudOssUtil.getUddStatusDataKey(bean.getOssFilePath, bean.getStatDate);
    val dateStrYtd = DateUtil.getDateStrBeforeNDays(DateUtil.convertStringToDate(bean.getStatDate), NumberConstant.INT_1)
    var parquetFilePathYtd: String = JdCloudOssUtil.getUddStatusDataKey(bean.getOssFilePath, dateStrYtd);
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
    var statusYtdDF: Dataset[Row] = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    if (!jfsClient.hasBucket(bean.getBucket)) {
      throw new JobInterruptedException("bucket不存在bucketname=" + bean.getBucket, "Exception: The Bucket " + bean.getBucket + " is not exist")
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
    if (JdCloudOssUtil.objectExist(jfsClient, bean.getBucket, parquetFilePathYtd)) {
      //数据存在
      val pathYtd:String = bean.getBucket + File.separator + parquetFilePathYtd
      val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
      readconfig.setOptions(new HashMap[String, String]() {{put("header", "true")}})
      readconfig.setClientType(ConfigProperties.getClientType)
      readconfig.setFileType(OssFileTypeEnum.parquet)
      readconfig.setObjectKey(pathYtd)
      statusYtdDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, readconfig)
//      statusYtdDF = JdCloudOssUtil.readDatasetFromOss(sparkSession, bean.getEndPoint, bean.getAccessKey, bean.getSecretKey, pathYtd, OssFileTypeEnum.parquet, ConfigProperties.getClientType)
    }
    //2. 分层数据计算
    val assetsDf = getAssetsDf(statusDF, statusYtdDF, sparkSession);
    val formatAssetsDf = getFormatAssetsDf(assetsDf, bean.getStatDate)
    assetsDfToSave(formatAssetsDf, 1, bean.getEsIndex)
    //3. 元数据信息返回
    val resultMap = new java.util.HashMap[String, Object]()
    LOGGER.info("resultMap=" + resultMap.toString)
    resultMap
  }

  /**
   * 获取分层结果数据
   *
   * @param statusDF
   * @param statusDFYtd
   * @param sparkSession
   * @return
   */
  private def getAssetsDf(statusDF: Dataset[Row], statusDFYtd: Dataset[Row], sparkSession: SparkSession): DataFrame = {
    val statusViewName = "status_view_table"
    val statusYtdViewName = "status_ytd_view_table"

    statusDF.createOrReplaceTempView(statusViewName)
    statusDFYtd.createOrReplaceTempView(statusYtdViewName)
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
         |      status
         |    from $statusViewName
         |  ) t1
         |full outer join
         |  (
         |    select
         |      dataset_id,
         |      user_id,
         |      status
         |    from $statusYtdViewName
         |  ) t2
         |on t1.dataset_id=t2.dataset_id AND t1.user_id=t2.user_id
         |group by case when t1.dataset_id is not null then t1.dataset_id else t2.dataset_id end
       """.stripMargin.replaceAll("\n", " ")

    sparkSession.sql(statusSql)
  }

  /**
   * 分布DF格式化，添加静态字段
   *
   * @param assetsDf DataFrame
   * @param dateStr  String
   * @return
   */
  private def getFormatAssetsDf(assetsDf: DataFrame, dateStr: String): DataFrame = {
    assetsDf.withColumn("dt", lit(dateStr))
  }

  /**
   * 结果集入库
   *
   * @param dataFrame      DataFrame
   * @param numPartsToSave Int
   */
  private def assetsDfToSave(dataFrame: DataFrame, numPartsToSave: Int, esIndex: String): Unit = {
    val dfToWriter = dataFrame.select(resultColumns.head, resultColumns.tail: _*).repartition(numPartsToSave)
    dfToWriter.cache()
    LOGGER.info("assetsDfToSave write data to es count:{}", dfToWriter.count())
    val dfToEs = dfToWriter
      .withColumnRenamed("dataset_id", "datasetId")
      .withColumnRenamed("a1_cust_count", "a1CustCount")
      .withColumnRenamed("a1_cust_inflow", "a1CustInFlow")
      .withColumnRenamed("a1_cust_outflow", "a1CustOutFlow")
      .withColumnRenamed("a2_cust_count", "a2CustCount")
      .withColumnRenamed("a2_cust_inflow", "a2CustInFlow")
      .withColumnRenamed("a2_cust_outflow", "a2CustOutFlow")
      .withColumnRenamed("a3_cust_count", "a3CustCount")
      .withColumnRenamed("a3_cust_inflow", "a3CustInFlow")
      .withColumnRenamed("a3_cust_outflow", "a3CustOutFlow")
      .withColumnRenamed("a4_cust_count", "a4CustCount")
      .withColumnRenamed("a4_cust_inflow", "a4CustInFlow")
      .withColumnRenamed("a4_cust_outflow", "a4CustOutFlow")
      .withColumnRenamed("dt", "processDate")
      .withColumn("version", lit(ConfigProperties.VERSION))
      .withColumn("id", concat_ws("-", col("processDate"), col("datasetId")))

    EsSparkSQL.saveToEs(dfToEs, esIndex, Map("es.mapping.id" -> "id"))
  }

  private def deleteEsData(bean: UddAssetsBean): Unit = {
    val condDatasetId: SqlCondition = new SqlCondition
    condDatasetId.setColumnName("datasetId")
    condDatasetId.setParam1(bean.getDatasetId)
    condDatasetId.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condDatasetId.setLogicalOperator(LogicalOperatorEnum.and)
    val condEndDate = new SqlCondition
    condEndDate.setColumnName("processDate")
    condEndDate.setParam1(bean.getStatDate)
    condEndDate.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condEndDate.setLogicalOperator(LogicalOperatorEnum.and)
    val conds: java.util.List[SqlCondition] = new java.util.ArrayList[SqlCondition]() {{
      add(condDatasetId)
      add(condEndDate)
    }}
    val dslJson: JSONObject = EsUtil.queryDslJSON(conds)
    val esCluster = new EsCluster
    esCluster.setEsNodes(bean.getEsNode)
    esCluster.setEsPort(bean.getEsPort)
    esCluster.setUsername(bean.getEsUser)
    esCluster.setPassword(bean.getEsPassword)
    esCluster.setEsIndex(bean.getEsIndex)
    EsUtil.deleteEsData(esCluster, dslJson)
    LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString)
  }
//  /**
//   * 任务运行前提前写入空数据
//   *
//   * @param sparkSession
//   */
//  private def writeEmptyDataToEs(sparkSession: SparkSession): Unit = {
//    val resultSchema = StructType(resultColumns.map(fieldName => {
//      if (fieldName.equalsIgnoreCase("dt")) {
//        StructField(fieldName, DataTypes.StringType, true)
//      } else {
//        StructField(fieldName, DataTypes.LongType, true)
//      }
//    }))
//    var data: java.util.List[Row] = Lists.newArrayList(Row(getDatasetId.toLong, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, getStatDate))
//    var emptyData = sparkSession.createDataFrame(data, resultSchema)
//    LOGGER.info("ready to write emptydata to es")
//    emptyData.show(10)
//    assetsDfToSave(emptyData, 1)
//
//  }
}