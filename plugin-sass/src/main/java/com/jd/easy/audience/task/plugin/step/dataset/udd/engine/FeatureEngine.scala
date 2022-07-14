package com.jd.easy.audience.task.plugin.step.dataset.udd.engine

import java.util.{ArrayList, HashMap, List, Map, UUID}
import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.model.UddModel
import com.jd.easy.audience.common.model.param.{DatasetUserIdConf, FeatureFiledModel, RelDatasetModel}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.step.dataset.udd.util.DateUtil
import com.jd.easy.audience.task.plugin.util.CommonDealUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import java.util
import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

/**
 * 特征解析引擎
 *
 * @param udd UddModel
 */
class FeatureEngine(udd: UddModel, isOneIdRet: Boolean)(implicit spark: SparkSession) extends Serializable {

  /**
   * 数据源维度字段（统一名称）
   * Map[tbName, user_id&dt]
   */
  private val tbDimInfoMap = getTbDimInfoMap


  /**
   * 特征计算入口，采集特征数据
   *
   * @param dateStr String
   * @return [user_id, feature_map]  dDataFrame
   */
  def executeNormal(dateStr: String): DataFrame = {

    println("enter FeatureEngine.excute...")
    //1. 初始化
    var featureDf = spark.emptyDataFrame
    //2. 加工特征对象集合Seq[FeatureGeneratorInfo]
    val featureGeneratorInfoSet = getFeatureInfo
    println("featureGeneratorInfoSet.size=" + featureGeneratorInfoSet.size)
    println("featureGeneratorInfoSet.info=" + featureGeneratorInfoSet.toString())
    for ((tableName, featurePeriodInfo) <- featureGeneratorInfoSet) {

      //3. 同一表的数据 featureGeneratorInfo=Map[tbname, Map[period, Set(feature对象)] ]
      var tbFeatureDf = spark.emptyDataFrame
      for ((period, featureSet) <- featurePeriodInfo) {
        val featureGeneratorInfo = new FeatureGeneratorInfo(tableName, period, featureSet.toSet)
        println("featureGeneratorInfo===>", featureGeneratorInfo.toString)
        println("tableName===>", featureGeneratorInfo.tableName)

        //4. 进行特征采集，计算具体的特征值
        var userIdInfo: Tuple2[String, String] = tbDimInfoMap(tableName)
        val tbPeriodFeatureDf = new FeatureCollector(featureGeneratorInfo).execute(dateStr, userIdInfo)
        tbFeatureDf = SparkLauncherUtil.dfUnion(spark, tbFeatureDf, tbPeriodFeatureDf)
      }

      //单张表的所有特征进行收集
      featureDf = SparkLauncherUtil.dfUnion(spark, featureDf, tbFeatureDf)
    }
    featureDf
  }

  def dealOneCofig(isTest: Boolean): Map[String, List[Tuple2[String, java.lang.Long]]] = {
    if (!spark.catalog.tableExists(ConfigProperties.ONEID_DETAIL_TB)) {
      throw new JobInterruptedException(s"""${ConfigProperties.ONEID_DETAIL_TB} 表不存在，请重新检查""", "Failure: The Table about oneid is not exist!")
    }
    //one_id配置信息处理
    var userFieldMap: Map[String, List[Tuple2[String, java.lang.Long]]] = new HashMap[String, List[Tuple2[String, java.lang.Long]]]()
    var config: util.Map[String, util.List[DatasetUserIdConf]] = new util.HashMap[String, util.List[DatasetUserIdConf]]
    if (isTest) {
      config.put("one_id_4a_8_9", new util.ArrayList[DatasetUserIdConf]() {
        val conf1 = new DatasetUserIdConf
        conf1.setUserIdType(8L)
        conf1.setUserField("user_8")
        add(conf1)
        val conf2 = new DatasetUserIdConf
        conf2.setUserIdType(9L)
        conf2.setUserField("user_9")
        add(conf2)
      })
      config.put("one_id_4a_9_10", new util.ArrayList[DatasetUserIdConf]() {
        val conf1 = new DatasetUserIdConf
        conf1.setUserIdType(10L)
        conf1.setUserField("user_10")
        val conf2 = new DatasetUserIdConf
        conf2.setUserIdType(9L)
        conf2.setUserField("user_9")
        add(conf1)
        add(conf2)
      })
    }
    else {
      config = CommonDealUtils.getSourceOneIdConfig(udd.getRelDataset.getData, StringConstant.EMPTY)
    }
    udd.getRelDataset.getData.foreach(model => {
      val tablename = model.getDataTable
      val oneIdConf: List[Tuple2[String, java.lang.Long]] = config.get(tablename).map(conf => {
        (conf.getUserField, conf.getUserIdType)
      }).toList
      userFieldMap += ((model.getDataTable) -> oneIdConf)
    })
    //one_id明细数据
    userFieldMap
  }

  /**
   * 特征计算入口，采集特征数据-使用one_id
   *
   * @param dateStr String
   * @return [user_id, feature_map]  dDataFrame
   */
  def executeOneId(dateStr: String, isTest: Boolean): DataFrame = {
    val isUseOneId = isOneIdRet || udd.getRelDataset.getData.filter(source => source.isUseOneId).size > NumberConstant.INT_0
    var userFieldMap: Map[String, List[Tuple2[String, java.lang.Long]]] = new HashMap[String, List[Tuple2[String, java.lang.Long]]]()
    var oneIdDs = spark.emptyDataFrame
    val featureGeneratorInfoSet = getFeatureInfo
    /**
     * Map(tablename1->(userid,dt),tablename2->(userid2,dt2))
     */
    var tableInfoConfig: Map[String, Tuple3[String, String, String]] = new HashMap[String, Tuple3[String, String, String]]()
    if (isUseOneId) {
      /**
       * 需要进行oneid处理
       */
      userFieldMap = dealOneCofig(isTest)
      oneIdDs = spark.read.table(ConfigProperties.ONEID_DETAIL_TB)
      oneIdDs.cache()
    }
    //2. 特征信息处理
    udd.getRelDataset.getData.foreach(relSource => {
      val tableName = relSource.getDataTable
      breakable {
        if (!featureGeneratorInfoSet.contains(tableName)) {
          //2.1 该数据源未配置特征信息
          break
        }
        if (relSource.isUseOneId || (isOneIdRet && udd.getRelDataset.getData.size() == NumberConstant.INT_1)) {
          //2.2 该数据源需要和oneid进行关联
          val featurePeriodInfo = featureGeneratorInfoSet.get(tableName).get
          val maxPeriod = featurePeriodInfo.keySet.max
          val dtField = tbDimInfoMap(tableName)._2
          val sqlOrigin =
            s"""
               |SELECT
               |  *
               |FROM $tableName WHERE
               |  $dtField > "${DateUtil.getDateStrNextNDays(dateStr, -maxPeriod)}" AND $dtField <= "$dateStr"
     """.stripMargin
          println("原始表数据过滤 = " + sqlOrigin)
        val dataOrigin = spark.sql(sqlOrigin)
        var viewConfig: Tuple2[String, String] = dealOneIdJoin(dataOrigin, oneIdDs, userFieldMap(tableName))
        tableInfoConfig.put(tableName, Tuple3(viewConfig._1, viewConfig._2, tbDimInfoMap(tableName)._2))
        } else {
          tableInfoConfig.put(tableName, Tuple3(tableName, tbDimInfoMap(tableName)._1, tbDimInfoMap(tableName)._2))
        }
      }
    })

    //3. 特征数据处理
    var featureDf = spark.emptyDataFrame
    for ((tableName, featurePeriodInfo) <- featureGeneratorInfoSet) {
      var tbFeatureDf = spark.emptyDataFrame
      //3.1 进行特征采集，计算具体的特征值
      for ((period, featureSet) <- featurePeriodInfo) {
        val featureGeneratorInfo = new FeatureGeneratorInfo(tableInfoConfig.get(tableName)._1, period, featureSet.toSet)
        println("featureGeneratorInfo===>", featureGeneratorInfo.toString)
        println("tableName===>", featureGeneratorInfo.tableName)
        var userIdInfo = (tableInfoConfig.get(tableName)._2, tbDimInfoMap(tableName)._2)
        val tbPeriodFeatureDf = new FeatureCollector(featureGeneratorInfo).execute(dateStr, userIdInfo)
        tbFeatureDf = SparkLauncherUtil.dfUnion(spark, tbFeatureDf, tbPeriodFeatureDf)
      }
      //3.2 特征数据汇总
      featureDf = SparkLauncherUtil.dfUnion(spark, featureDf, tbFeatureDf)
    }
    featureDf
  }

  /**
   * 获取数据源维度字段 Map[tbName, (user_id, dt)]
   */
  private def getTbDimInfoMap: Map[String, (String, String)] = {
    var tbDimInfo = new HashMap[String, (String, String)]()
    //    if (udd.getRelDataset.getData.size() == NumberConstant.INT_1) {
    //      val datasource = udd.getRelDataset.getData.get(0)
    //      tbDimInfo += (datasource.getDataTable -> Tuple2(udd.getFeatureMapping.getUserFlag, datasource.getDateField))
    //      return tbDimInfo
    //    }
    for (tbInfo <- udd.getRelDataset.getData) {
      val tbName = s"${tbInfo.getDataTable}"
      val dt = s"${tbInfo.getDateField}"
      val userIdField = s"${tbInfo.getRelationField}"
      tbDimInfo += (tbName -> Tuple2(userIdField, dt))
    }
    tbDimInfo
  }

  /**
   * 和oneid表进行关联后再加工
   *
   * @param originDf
   * @param oneIdviewName
   * @param userIdConf
   * @return
   */
  private def dealOneIdJoin(originDf: Dataset[Row], oneIdviewName: Dataset[Row], userIdConf: List[Tuple2[String, java.lang.Long]]): (String, String) = {
    var nextOrigin: Dataset[Row] = originDf
    val sourceCnt: Integer = userIdConf.size
    var resultData: Dataset[Row] = spark.emptyDataFrame
    //1. 命名关联字段和oneID字段
    val relationField = "relation_" + UUID.randomUUID().toString.replaceAll(StringConstant.MIDDLELINE, "")
    val oneIdField = "oneid_" + UUID.randomUUID().toString.replaceAll(StringConstant.MIDDLELINE, "")
    //2. 根据oneId优先级配置选取用户标示字段进行关联
    for (i <- 0 until sourceCnt) {
      val (userIdField, idType) = userIdConf.get(i)
      //当前参与关联的用户标示字段
      val curOrigin: Dataset[Row] = nextOrigin.filter(userIdField + " IS NOT NULL AND " + userIdField + " !=\"\"").withColumn(relationField, functions.concat(functions.col(userIdField), functions.lit("_" + idType)))
      nextOrigin = nextOrigin.filter(userIdField + " IS  NULL OR " + userIdField + " =\"\"")
      if (resultData.isEmpty) {
        resultData = curOrigin
      } else if (!curOrigin.isEmpty) {
        resultData = resultData.unionAll(curOrigin)
      }
    }
    //3. 和oneid表进行关联
    val viewName = "source_" + UUID.randomUUID().toString.replaceAll(StringConstant.MIDDLELINE, "")
    resultData = resultData.as("tb_origin").join(oneIdviewName.selectExpr("one_id AS " + oneIdField, "concat(`id`,'_',`id_type`) AS " + relationField).as("tb_oneid"), relationField).drop(relationField)
    resultData.show(NumberConstant.INT_10)
    //处理字段信息
    resultData.createOrReplaceTempView(viewName)
    (viewName, oneIdField)
  }

  /**
   * 特征解析
   * Map[tbname, Map[period, Set(feature对象)] ]
   */
  private def getFeatureInfo: scala.collection.mutable.Map[String, scala.collection.mutable.Map[Int, scala.collection.mutable.Set[FeatureFiledModel]]] = {
    val tbPeriodFeatureSetMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[Int, scala.collection.mutable.Set[FeatureFiledModel]]]()
    for ((featureInfo, index) <- udd.getFeatureMapping.getData.zipWithIndex) {
      // 过滤userId和dt字段
      if (index > NumberConstant.INT_1) {
        val tb = s"${featureInfo.getSourceInfo.getTableName}"
        val period = featureInfo.getSourceInfo.getPeriod.toInt
        if (tbPeriodFeatureSetMap.contains(tb)) {
          if (tbPeriodFeatureSetMap(tb).contains(period)) {
            tbPeriodFeatureSetMap(tb)(period).add(featureInfo)
          } else {
            tbPeriodFeatureSetMap(tb) += (period -> scala.collection.mutable.Set(featureInfo))
          }
        } else {
          tbPeriodFeatureSetMap += (tb -> scala.collection.mutable.Map(period -> scala.collection.mutable.Set(featureInfo)))
        }
      }
    }
    tbPeriodFeatureSetMap
  }

}

/**
 * 特征info封装
 *
 * @param tableName  String
 * @param period     Int
 * @param featureSet Set[FeatureFiledModel]
 */
case class FeatureGeneratorInfo(tableName: String, period: Int, featureSet: Set[FeatureFiledModel]) {
  override def toString: String = return "FeatureGeneratorInfo{" +
    ", tableName='" + tableName + '\'' + ", period='" + period + '\'' + ", featureSet='" + featureSet.toString() + '\'' + '}'
}
