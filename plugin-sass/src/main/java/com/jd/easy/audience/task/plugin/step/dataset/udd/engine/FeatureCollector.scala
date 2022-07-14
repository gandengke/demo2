package com.jd.easy.audience.task.plugin.step.dataset.udd.engine

import com.jd.easy.audience.task.plugin.step.dataset.udd.util.DateUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * 特征采集器
 *
 * @param featureGeneratorInfo FeatureGeneratorInfo
 * @param spark                SparkSession
 */
class FeatureCollector(featureGeneratorInfo: FeatureGeneratorInfo)(implicit spark: SparkSession) extends Serializable {
  private val LOGGER = LoggerFactory.getLogger(classOf[FeatureCollector])

  /**
   * 获取指标字段
   *
   * @return
   */
  private def getIndexFieldsStr: String = {
    val indexFieldList = scala.collection.mutable.ArrayBuffer[String]()
    //1. 遍历具体表具体周期的特征集合
    featureGeneratorInfo.featureSet.foreach(
      feature => {
        //e.g app_dm_4a_user_order_tb_ord_cnt_14
        indexFieldList.append("\"" + feature.getFieldName + "\"")
        //e.g. count(ord_cnt)
        indexFieldList.append(s"${feature.getFeatureExp}")
      }
    )
    //3. 特征字段组装成map
    s"map(${indexFieldList.mkString(", ")})"
  }

  /**
   * 特征采集：获取具体表，具体时间计算范围的特征集合，比如流量表的14天周期的特征
   *
   * @param dateStr      统计日期
   * @param tbDimInfo (user_id, dt)
   * @return
   */
  private def featureCollect(dateStr: String, tbDimInfo: (String, String)): DataFrame = {
    val tb = featureGeneratorInfo.tableName
    val userId = tbDimInfo._1
    val dt = tbDimInfo._2
    // e.g. map('fieldname1', count(ord_cnt), 'fieldname2', sum(view_cnt))
    val indexFieldsStr = getIndexFieldsStr
    LOGGER.info("featureCollect.tablename=" + tb)
    val sql =
      s"""
         |SELECT
         |  CAST($userId AS STRING) AS user_id,
         |  $indexFieldsStr AS feature_map
         |FROM $tb
         |WHERE
         |  $dt > '${DateUtil.getDateStrNextNDays(dateStr, -featureGeneratorInfo.period)}'
         |  AND $dt <= '$dateStr'
         |GROUP BY $userId
       """.stripMargin.replaceAll("\n", " ")
    LOGGER.info("featureCollect sql=" + sql)

    spark.sql(sql)
  }

  /**
   * 特征采集器入口类
   *
   * @param dateStr      String
   * @param tbDimInfo Map[tbName, (user_id, dt)]
   * @return
   */
  def execute(dateStr: String, tbDimInfo: (String, String)): DataFrame = {
    LOGGER.info("enter FeatureCollector.excute...")

    featureCollect(dateStr, tbDimInfo).repartition(800)
  }

}
