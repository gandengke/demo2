package com.jd.easy.audience.task.plugin.step.dataset.rfm

import com.alibaba.fastjson.JSONObject
import com.jd.easy.audience.common.constant.{LogicalOperatorEnum, NumberConstant, RFMDataTypeEnum, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.model.param.{DatasetUserIdConf, RelDatasetModel}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.{RfmAlalysisBean, RfmAnalysisConfig}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.generator.util.DateUtil
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.RfmAnalysis
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo.{RfmAnalysisConfigInfo, RfmAnalysisTaskNew}
import com.jd.easy.audience.task.plugin.util.es.EsUtil
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster
import com.jd.easy.audience.task.plugin.util.es.sqltool.{ComparisonOperatorsEnum, SqlCondition}
import com.jd.easy.audience.task.plugin.util.{CommonDealUtils, SparkUdfUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.util
import java.util.{ArrayList, HashMap, List, Map}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable

/**
 * 4a明细数据
 */
class RfmDatasetStep extends StepCommon[Unit] {
  private val LOGGER = LoggerFactory.getLogger(classOf[RfmDatasetStep])
  /**
   * 时间分区字段偏移的天数
   */
  private val DT_FILTER_DAYS = NumberConstant.INT_60
  /**
   * 用户数据的schema
   */
  private[rfm] val userfields: List[StructField] = new util.LinkedList[StructField]() {
    {
      add(DataTypes.createStructField("user_log_acct", DataTypes.StringType, true))
      add(DataTypes.createStructField("frequency", DataTypes.IntegerType, true))
      add(DataTypes.createStructField("monetary", DataTypes.DoubleType, true))
      add(DataTypes.createStructField("amount", DataTypes.IntegerType, true))
      add(DataTypes.createStructField("channel", DataTypes.createArrayType(DataTypes.StringType, true), true))
      add(DataTypes.createStructField("first_dt", DataTypes.StringType, true))
      add(DataTypes.createStructField("latest_dt", DataTypes.StringType, true))
    }
  }
  /**
   * 交易数据的schema
   */
  private[rfm] val dealfields: List[StructField] = new util.LinkedList[StructField]() {
    {
      add(DataTypes.createStructField("user_log_acct", DataTypes.StringType, true))
      add(DataTypes.createStructField("parent_sale_ord_id", DataTypes.StringType, true))
      add(DataTypes.createStructField("monetary", DataTypes.DoubleType, true))
      add(DataTypes.createStructField("amount", DataTypes.IntegerType, true))
      add(DataTypes.createStructField("channel", DataTypes.StringType, true))
      add(DataTypes.createStructField("dt", DataTypes.StringType, true))
    }
  }

  override def validate(): Unit = {
    val bean: RfmAlalysisBean = getStepBean.asInstanceOf[RfmAlalysisBean];
    if (StringUtils.isAnyBlank(bean.getRfmAnalysisConfig.getJfsEndPoint, bean.getRfmAnalysisConfig.getJfsBucket, bean.getRfmAnalysisConfig.getAccessKey, bean.getRfmAnalysisConfig.getSecretKey, bean.getOutputPath)) {
      throw new JobInterruptedException("RfmDatasetStep oss配置参数错误", "Error: The Oss_Configuration of RfmDatasetStep Error");
    } else if (null == bean.getRelDataset || null == bean.getEndDate) {
      throw new JobInterruptedException("RfmDatasetStep 模型配置参数错误", "Error: The data source of RfmDatasetStep Error");
    }
  }

  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: java.util.Map[String, StepCommonBean[_]]): Unit = {
    //1. 初始化
    LOGGER.error("RfmDatasetStep start!")
    val bean: RfmAlalysisBean = getStepBean.asInstanceOf[RfmAlalysisBean];
    implicit val sparkSession = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getRfmAnalysisConfig.getEsNode, bean.getRfmAnalysisConfig.getEsPort, bean.getRfmAnalysisConfig.getEsUser, bean.getRfmAnalysisConfig.getEsPassword, bean.getStepName);

    val dbName = bean.getRelDataset.getData.get(0).getDatabaseName
    sparkSession.sql("use " + dbName)
    //2. 校验表是否存在
    val tableList = bean.getRelDataset.getData
    for (i <- NumberConstant.INT_0 until tableList.size) {
      val tb = tableList(i)
      if (!sparkSession.catalog.tableExists(tb.getDatabaseName, tb.getDataTable)) {
        throw new JobInterruptedException(s"""${tb.getDataTable} 表不存在，请重新检查""", "Failure: The Table " + tb.getDataTable + " is not exist!")
      }
    }
    //3. 清理oss+es数据
    deleteEsData(bean)
    val jfsClient = JdCloudOssUtil.createJfsClient(bean.getRfmAnalysisConfig.getJfsEndPoint, bean.getRfmAnalysisConfig.getAccessKey, bean.getRfmAnalysisConfig.getSecretKey)
    JdCloudOssUtil.deleteOssDir(jfsClient, bean.getRfmAnalysisConfig.getJfsBucket, bean.getOutputPath, null)
    jfsClient.destroy()
    //4. 获取数据
    val dataset = buildSqlNew(sparkSession, bean, false).repartition(NumberConstant.INT_10)
    //5.模型调用
    computeAlalysis(sparkSession, dataset, bean)
  }

  /**
   * 处理每个数据源需要筛选的字段
   *
   * @param bean
   * @param tableAlias
   * @return
   */
  def filterSubTableField(bean: RfmAlalysisBean, tableAlias: util.Map[String, String]): Map[String, List[String]] = {
    val fieldList: util.Map[String, List[String]] = new HashMap[String, List[String]]()
    val mainTable: String = bean.getUserIdField.split("\\.")(1)
    //1. 添加关联字段
    bean.getRelDataset.getData.foreach(source => if (!source.isUseOneId && StringUtils.isNotBlank(source.getRelationField)) {
      val labelist: List[String] = fieldList.getOrDefault(tableAlias.get(source.getDataTable), new util.ArrayList[String]())
      labelist.add(source.getRelationField + " AS relation_id")
      fieldList.put(tableAlias.get(source.getDataTable), labelist)

    })
    //2. 添加user_id字段
    if (!bean.isUseOneId) {
      val labelist: List[String] = fieldList.getOrDefault(tableAlias.get(mainTable), new util.ArrayList[String]())
      labelist.add(bean.getUserIdField.split("\\.")(2) + " AS user_id")
      fieldList.put(tableAlias.get(mainTable), labelist)
    }
    //3. rfm配置相关字段
    if (bean.getDataType.getValue.equalsIgnoreCase(RFMDataTypeEnum.user.getValue)) {
      //3.1 用户数据
      parseFieldAlias(bean.getFrequencyField, "frequency", fieldList, tableAlias)
      parseFieldAlias(bean.getMontaryField, "monetary", fieldList, tableAlias)
      parseFieldAlias(bean.getRecentyField, "latest_dt", fieldList, tableAlias)
    }
    else {
      //3.2 交易数据
      parseFieldAlias(bean.getSaleIdField, "parent_sale_ord_id", fieldList, tableAlias)
      parseFieldAlias(bean.getMontaryField, "monetary", fieldList, tableAlias)
      parseFieldAlias(bean.getOrderDtField, "dt", fieldList, tableAlias)
    }
    fieldList
  }

  private def filterExpress(bean: RfmAlalysisBean, sourceInfo: RelDatasetModel) = {
    val str = new mutable.StringBuilder(sourceInfo.getDateField)
    var recentField = ""

    /**
     * 分区字段过滤
     */
    if (bean.getDataType.getValue.equalsIgnoreCase(RFMDataTypeEnum.deal.getValue)) { //分区字段范围
      val dayHis = DateUtil.addDays(bean.getEndDate, -DT_FILTER_DAYS - bean.getPeriodDays + BigInteger.ONE.intValue)
      str.append(" >= \"").append(dayHis).append("\"")
      recentField = bean.getOrderDtField
    }
    else if (bean.getDataType.getValue eq RFMDataTypeEnum.user.getValue) { //用户数据取统计某一天分区的数据
      str.append(" = \"").append(bean.getEndDate).append("\"")
      recentField = bean.getRecentyField
    }
    /**
     * 分析周期数据过滤（交易数据使用下单日期字段，用户数据使用最近下单字段过滤）
     */
    val recentTb = recentField.split("\\.")(1)
    if (sourceInfo.getDataTable.equalsIgnoreCase(recentTb)) {
      val dayHisOrder = DateUtil.addDays(bean.getEndDate, -bean.getPeriodDays + BigInteger.ONE.intValue)
      val fieldName = recentField.split("\\.")(2)
      str.append(" AND ").append(fieldName).append(" >= \"").append(dayHisOrder).append("\"")
      str.append(" AND ").append(fieldName).append(" <= \"").append(bean.getEndDate).append("\"")
    }

    /**
     * 过滤普通字段关联，但关联字段为空的数据
     */
    if (StringUtils.isNotBlank(sourceInfo.getRelationField) && !sourceInfo.isUseOneId) str.append(" AND " + sourceInfo.getRelationField + " is not null AND " + sourceInfo.getRelationField + "!=''")
    str.toString
  }

  private def getOneIdConfigInfo(bean: RfmAlalysisBean, isTest: Boolean) = {
    var config = new util.HashMap[String, util.List[DatasetUserIdConf]]
    if (isTest) {
      config.put("oneid_rfm_user_8_9", new util.ArrayList[DatasetUserIdConf]() {
        {
          val conf1 = new DatasetUserIdConf
          conf1.setUserIdType(8L)
          conf1.setUserField("user_8")
          add(conf1)
          val conf2 = new DatasetUserIdConf
          conf2.setUserIdType(9L)
          conf2.setUserField("user_9")
          add(conf2)
        }
      })
      config.put("oneid_rfm_user_9_10", new util.ArrayList[DatasetUserIdConf]() {
        {
          val conf1 = new DatasetUserIdConf
          conf1.setUserIdType(9L)
          conf1.setUserField("user_9")
          val conf2 = new DatasetUserIdConf
          conf2.setUserIdType(10L)
          conf2.setUserField("user_10")
          add(conf1)
          add(conf2)
        }
      })
    }
    else {
      var userIdTb = ""
      if (bean.isUseOneId) userIdTb = bean.getUserIdField.replaceAll("\\.OneID", "")
      config = CommonDealUtils.getSourceOneIdConfig(bean.getRelDataset.getData, userIdTb)
    }
    config
  }

  /**
   * 处理关键配置字段
   *
   * @param fieldName
   * @param newFieldName
   * @param tableField
   * @param tableAlias
   */
  private def parseFieldAlias(fieldName: String, newFieldName: String, tableField: util.Map[String, util.List[String]], tableAlias: util.Map[String, String]): Unit = {
    val fieldPure = fieldName.split("\\.")(2)
    val tableName = fieldName.split("\\.")(1)
    val adderConfig = new java.util.function.Function[String, util.List[String]]() {
      override def apply(t: String) = new util.ArrayList[String]()
    }
    val labelist = tableField.computeIfAbsent(tableAlias.get(tableName), adderConfig)
    labelist.add(fieldPure + " AS " + newFieldName)
  }

  /**
   * 处理每个数据源的子表表达式信息
   *
   * @param spark
   * @param bean
   * @param tableAlias
   * @param isTest
   * @return
   */
  private def dealOneJoinSubSql(spark: SparkSession, bean: RfmAlalysisBean, tableAlias: Map[String, String], isTest: Boolean): util.LinkedList[String] = {
    //1. oneID明细表和配置信息获取
    val isOneIdJoin: Boolean = bean.getRelDataset.getData.filter(datasource => datasource.isUseOneId).size > NumberConstant.INT_0
    var oneIdDs: Dataset[Row] = spark.emptyDataFrame
    var config: Map[String, List[DatasetUserIdConf]] = new util.HashMap[String, List[DatasetUserIdConf]]
    if (isOneIdJoin || bean.isUseOneId) {
      LOGGER.error("查询oneID表")
      oneIdDs = spark.read.table(ConfigProperties.ONEID_DETAIL_TB)
      oneIdDs.cache
      config = getOneIdConfigInfo(bean, isTest)
    }
    /**
     * 2. 存放子表的表达式
     * 1) 如果使用普通字段关联，则存具体的子表表达式
     * 2) 如果使用oneID关联，则存表别名
     */
    val tableFilterExp: util.LinkedList[String] = new util.LinkedList[String]
    //2.1 处理需要筛选的字段
    val tableFields: Map[String, List[String]] = filterSubTableField(bean, tableAlias)
    val mainTable: String = bean.getUserIdField.split("\\.")(1)
    for (sourceInfo <- bean.getRelDataset.getData) {
      val tableName: String = sourceInfo.getDataTable

      //2.2 需要进行oneid关联，扩展用户标示字段集合
      val isNeedOneIdJoin: Boolean = sourceInfo.isUseOneId || (bean.isUseOneId && tableName.equalsIgnoreCase(mainTable))
      if (isNeedOneIdJoin) {
        val userIds: List[String] = SparkUdfUtil.getUserIdsFromOneId(config.get(tableName))
        val labelist: List[String] = tableFields.getOrDefault(tableAlias.get(tableName), new ArrayList[String])
        labelist.addAll(userIds)
        tableFields.put(tableAlias.get(tableName), labelist)
      }
      val strChildTbSql: String =
        s"""
           |SELECT
           |  ${StringUtils.join(tableFields.get(tableAlias.get(tableName)), StringConstant.COMMA)}
           |FROM
           |  ${tableName}
           |WHERE
           |  ${filterExpress(bean, sourceInfo)}
           |""".stripMargin
      var subTableExpress: String = ""
      if (!isNeedOneIdJoin) {
        //2.3 无需oneID关联，直接以sql语句返回
        subTableExpress = " (" + strChildTbSql + ") " + tableAlias.get(tableName)
      } else {
        //2.4 需要关联oneID
        LOGGER.error("需要进行oneID关联，tableName=" + tableName + ",sql:" + strChildTbSql)
        val dataOrigin: Dataset[Row] = spark.sql(strChildTbSql)
        var dataset = oneIdTempData(dataOrigin, config.get(tableName), oneIdDs)
        if (sourceInfo.isUseOneId) {
          dataset = dataset.withColumn("relation_id", functions.col("one_id"))
        }
        if (bean.isUseOneId && tableName.equalsIgnoreCase(mainTable)) {
          dataset = dataset.withColumn("user_id", functions.col("one_id"))
        }
        dataset.cache

        val oneIdJoinView: String = tableAlias.get(tableName)
        LOGGER.error("生成临时表：" + oneIdJoinView)
        if (isTest) {
          LOGGER.error("table view:" + oneIdJoinView)
          dataset.show(NumberConstant.INT_10)
          dataset.printSchema()
        }
        dataset.createOrReplaceTempView(oneIdJoinView)
        subTableExpress = oneIdJoinView
      }
      //3. 对多张表进行关联
      if (tableName.equalsIgnoreCase(mainTable)) {
        tableFilterExp.addFirst(subTableExpress)
      }
      else {
        var joinSubSql: String = ""
        joinSubSql += " ON " + tableAlias.get(mainTable) + ".relation_id="
        joinSubSql += tableAlias.get(tableName) + ".relation_id"
        tableFilterExp.add(subTableExpress + joinSubSql)
      }
    }
    tableFilterExp
  }

  /**
   * 根据oneid配置优先级进行oneid数据关联
   *
   * @param dataOrigin
   * @return
   */
  def oneIdTempData(dataOrigin: Dataset[Row], config: List[DatasetUserIdConf], oneIdDs: Dataset[Row]): Dataset[Row] = {
    val columns: Array[String] = dataOrigin.columns
    val columnsMap: Map[String, String] = new util.HashMap[String, String]
    val renameMap: Map[String, String] = new util.HashMap[String, String]
    columns.map(col => {
      columnsMap.put(col, "max")
      renameMap.put("max(" + col + ")", col)
    })
    var nextOrigin: Dataset[Row] = dataOrigin
    val sourceCnt: Integer = config.size
    var resultData: Dataset[Row] = null

    /**
     * resultData
     * 1.根据oneid配置的优先级，新增关联字段relation_field
     */
    for (i <- 0 until sourceCnt) {
      val oneIdConf: DatasetUserIdConf = config.get(i)
      //当前参与关联的用户标示字段
      val curuseridField: String = "user_" + oneIdConf.getUserIdType
      val curOrigin: Dataset[Row] = nextOrigin.filter(curuseridField + " IS NOT NULL AND " + curuseridField + " !=\"\"").withColumn("relation_field", functions.concat(functions.col(curuseridField), functions.lit("_" + oneIdConf.getUserIdType)))
      nextOrigin = nextOrigin.filter(curuseridField + " IS  NULL OR " + curuseridField + " =\"\"")
      if (null == resultData) {
        resultData = curOrigin
      }
      else {
        if (!curOrigin.isEmpty) {
          resultData = resultData.unionAll(curOrigin)
        }
      }
    }
    //和oneid表进行关联
    var dataset: Dataset[Row] = resultData.join(oneIdDs.selectExpr("one_id", "concat(`id`,'_' ,`id_type`) AS relation_field"), "relation_field")
    // 使用不同字段关联oneid，可能出现重复用户，对用户进行去重
    dataset = dataset.groupBy("one_id").agg(columnsMap)
    import scala.collection.JavaConversions._
    for (colName <- renameMap.keySet) {
      dataset = dataset.withColumnRenamed(colName, renameMap.get(colName))
    }
    dataset
  }

  /**
   * 根据配置信息加工模型需要的标准格式的数据
   *
   * @param spark
   * @param bean
   * @param isTest
   * @return
   */
  def buildSqlNew(spark: SparkSession, bean: RfmAlalysisBean, isTest: Boolean): Dataset[Row] = {
    //1. 对应表的别名
    val tableAlias: util.Map[String, String] = new util.HashMap[String, String](NumberConstant.INT_20)
    val tableList: util.List[RelDatasetModel] = bean.getRelDataset.getData
    for (i <- BigInteger.ZERO.intValue until tableList.size) {
      val table: RelDatasetModel = tableList.get(i)
      val tableAliasName: String = "tbl_" + i
      tableAlias.put(table.getDataTable, tableAliasName)
    }
    //2. 获取每个字表的查询
    var tableFilterExp: util.LinkedList[String] = dealOneJoinSubSql(spark, bean, tableAlias, isTest)

    //3. 拼接最终sql表达式
    var sqlFinal = ""
    val mainTable: String = bean.getUserIdField.split("\\.")(1)
    if (bean.getDataType.getValue eq RFMDataTypeEnum.user.getValue) {
      //3.1 用户数据
      sqlFinal =
        s"""
           |SELECT
           |  CAST(${tableAlias.get(mainTable)}.user_id AS STRING) AS user_log_acct,
           |  CAST(frequency AS INT) AS frequency,
           |  CAST(monetary AS DOUBLE) AS monetary,
           |  0 AS amount,
           |  array('other')  AS channel,
           |  '1900-01-01' AS first_dt,
           |  CAST(latest_dt AS STRING) AS latest_dt
           |FROM
           |  ${String.join(" JOIN ", tableFilterExp)}
           |""".stripMargin
    } else {
      //3.2 交易数据
      sqlFinal =
        s"""
           |SELECT
           |  CAST(${tableAlias.get(mainTable)}.user_id AS STRING) AS user_log_acct,
           |  CAST(parent_sale_ord_id AS STRING) AS parent_sale_ord_id,
           |  CAST(monetary AS DOUBLE) AS monetary,
           |  0 AS amount,
           |  'other' AS channel,
           |  CAST(dt AS STRING) AS dt
           |FROM
           |  ${String.join(" JOIN ", tableFilterExp)}
           |""".stripMargin
    }
    LOGGER.error("final sql = {}", sqlFinal)
    val dataset: Dataset[Row] = spark.sql(sqlFinal)
    dataset.cache
    //4. 数据结果大小校验
    if (dataset.count == BigInteger.ZERO.intValue) {
      throw new JobInterruptedException("rfm模型匹配数据为0", "Error: The size of RFM_Dataset is zero!")
    }
    //5. 格式化数据
    var schema: StructType = DataTypes.createStructType(userfields)
    var dsAlalysis: Dataset[Row] = spark.emptyDataFrame
    if (bean.getDataType.getType == RFMDataTypeEnum.user.getType) {
      dsAlalysis = dataset.select("user_log_acct", "frequency", "monetary", "amount", "channel", "first_dt", "latest_dt")
    } else {
      schema = DataTypes.createStructType(dealfields)
      dsAlalysis = dataset.select("user_log_acct", "parent_sale_ord_id", "monetary", "amount", "channel", "dt")
    }
    spark.createDataFrame(dsAlalysis.toJavaRDD, schema).filter("user_log_acct is not null and user_log_acct !=''")
  }

  /**
   * @throws
   * @title computeAlalysis
   * @description 参数封装，调用算法模块
   * @author cdxiongmei
   * @param: spark
   * @param: dataSet
   * @param: bean
   * @updateTime 2021/8/10 下午8:13
   */
  private def computeAlalysis(spark: SparkSession, dataSet: Dataset[Row], bean: RfmAlalysisBean): Unit = {
    val startDate = DateUtil.addDays(bean.getEndDate, -bean.getPeriodDays + BigInteger.ONE.intValue)
    val taskinfo = new RfmAnalysisTaskNew(bean.getDatasetId, bean.getOutputPath, startDate, bean.getEndDate, bean.getDataType.getType, dataSet, bean.getRfmAnalysisConfig.getJfsEndPoint, bean.getRfmAnalysisConfig.getAccessKey, bean.getRfmAnalysisConfig.getSecretKey)
    val config = new RfmAnalysisConfigInfo(bean.getRfmAnalysisConfig.getJfsBucket, bean.getRfmAnalysisConfig.getPurchaseBehaviorInfo, bean.getRfmAnalysisConfig.getPurchaseDurationDistribution, bean.getRfmAnalysisConfig.getPurchaseFrequencyDistribution, bean.getRfmAnalysisConfig.getRepurchaseCycleDistribution, bean.getRfmAnalysisConfig.getMonetaryDistribution, bean.getRfmAnalysisConfig.getPurchaseAmountDistribution, bean.getRfmAnalysisConfig.getPurchaseChannelDistribution, bean.getRfmAnalysisConfig.getConsumerLayer)
    val arrTask = new Array[RfmAnalysisTaskNew](NumberConstant.INT_1)
    arrTask(NumberConstant.INT_0) = taskinfo
    RfmAnalysis.compute(spark, config, arrTask)
  }

  private def deleteEsData(bean: RfmAlalysisBean): Unit = {
    val condDatasetId: SqlCondition = new SqlCondition
    condDatasetId.setColumnName("analyseId")
    condDatasetId.setParam1(bean.getDatasetId.toString)
    condDatasetId.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condDatasetId.setLogicalOperator(LogicalOperatorEnum.and)
    //endate
    val condEndDate: SqlCondition = new SqlCondition
    condEndDate.setColumnName("endDate")
    condEndDate.setParam1(bean.getDatasetId.toString)
    condEndDate.setCompareOpType(ComparisonOperatorsEnum.EQUALITY)
    condEndDate.setLogicalOperator(LogicalOperatorEnum.and)
    val conds: util.List[SqlCondition] = new util.ArrayList[SqlCondition]() {}
    val dslJson: JSONObject = EsUtil.queryDslJSON(conds)
    val esCluster: EsCluster = new EsCluster
    val esConfig: RfmAnalysisConfig = bean.getRfmAnalysisConfig
    esCluster.setEsNodes(esConfig.getEsNode)
    esCluster.setEsPort(esConfig.getEsPort)
    esCluster.setUsername(esConfig.getEsUser)
    esCluster.setPassword(esConfig.getEsPassword)
    esCluster.setEsIndex(esConfig.getPurchaseAmountDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getPurchaseChannelDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getPurchaseBehaviorInfo)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getPurchaseDurationDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getPurchaseFrequencyDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getRepurchaseCycleDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getMonetaryDistribution)
    EsUtil.deleteEsData(esCluster, dslJson)
    esCluster.setEsIndex(esConfig.getConsumerLayer)
    EsUtil.deleteEsData(esCluster, dslJson)
    LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString)
  }
}