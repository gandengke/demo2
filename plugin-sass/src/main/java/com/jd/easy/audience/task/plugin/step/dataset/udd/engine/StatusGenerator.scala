package com.jd.easy.audience.task.plugin.step.dataset.udd.engine

import com.jd.easy.audience.common.constant.NumberConstant
import com.jd.easy.audience.common.model.UddModel
import com.jd.easy.audience.task.plugin.step.dataset.udd.util.SparkUtil
import com.ql.util.express.ExpressRunner
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

/**
 * 用户分层入口类
 *
 * @param udd   UddModel
 * @param spark SparkSession
 */
class StatusGenerator(udd: UddModel, isOneIdRet: Boolean)(implicit spark: SparkSession) extends Serializable {

  private val ruleEngine = new RuleEngine(udd)

  private val featureEngine = new FeatureEngine(udd, isOneIdRet)

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

  /**
   * 入口func
   *
   * @param dateStr String
   * @return
   */
  def execute(dateStr: String, isTest: Boolean): DataFrame = {
    val statusDf = statusGenerator(dateStr, isTest)
    return statusDf
  }

  /**
   * udm status 计算器
   *
   * @param dateStr String
   * @return 4A明细DF
   */
  private def statusGenerator(dateStr: String, isTest: Boolean): DataFrame = {
    //1. 所有特征union[user_id, feature_map],需要按用户再次聚合，存在一个用户多条数据
    var featureDf = spark.emptyDataFrame
    var dataDf = spark.emptyDataFrame
    featureDf = featureEngine.executeOneId(dateStr, isTest)

    //用户特征汇总
    val featureDfRePart = featureDf.repartition(col("user_id"))
    dataDf = featureDfRePart.groupBy("user_id")
      .agg(collect_list("feature_map"))
    import spark.implicits._
    //2. 按user_id进行聚合
    val statusDf = dataDf
      .mapPartitions(iter => {
        //3. 表达式解析
        val expRunner = new ExpressRunner()
        var result = new ArrayBuffer[(String, Int, Map[String, Double], Map[String, Double], Map[String, Double], Map[String, Double], String, String)]()
        while (iter.hasNext) {
          val row = iter.next()
          val featureMapList = row.getAs[Seq[Map[String, Double]]](s"collect_list(feature_map)")
          println("statusGenerator->featureMapList:" + featureMapList.toString())
          //4. featureMap = Map[String, Double] 特征值进行处理
          val featureMap = mergeOneLevelMapList(featureMapList)
          //5. 特征是否满足条件Map[status, Map[field, featureValue]]
          val statusFeatureMap = ruleEngine.execute(expRunner, featureMap)
          val status = if (statusFeatureMap.nonEmpty) statusFeatureMap.keys.max else NumberConstant.INT_0

          result.+=(
            (
              row.getAs[String]("user_id"),
              status,
              statusFeatureMap.getOrElse(NumberConstant.INT_1, Map[String, Double]()),
              statusFeatureMap.getOrElse(NumberConstant.INT_2, Map[String, Double]()),
              statusFeatureMap.getOrElse(NumberConstant.INT_3, Map[String, Double]()),
              statusFeatureMap.getOrElse(NumberConstant.INT_4, Map[String, Double]()),
              dateStr,
              udd.getDatasetId.toString
            )
          )
        }
        result.iterator
      }).toDF(statusResultColumns: _*)
      .filter("status > 0")

    statusDf
  }

  /**
   * 将 fieldName->featureValue,需要进行控制的处理
   *
   * @param mapList
   * @return
   */
  def mergeOneLevelMapList(mapList: Seq[Map[String, AnyVal]]): Map[String, Double] = {
    var result = Map[String, Double]()
    val safeVal = (t: AnyVal) => Option(t).map(x => x.toString.toDouble).getOrElse(0D);
    for (map <- mapList) {
      result = result ++ map.map(t => t._1 -> (safeVal(t._2) + result.getOrElse(t._1, 0D)))
    }
    result
  }

}
