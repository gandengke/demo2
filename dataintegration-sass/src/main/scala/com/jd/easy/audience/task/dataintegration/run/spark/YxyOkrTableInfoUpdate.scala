package com.jd.easy.audience.task.dataintegration.run.spark

import com.jd.easy.audience.common.constant.NumberConstant
import com.jd.easy.audience.task.commonbean.contant.CATemplateEnum
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.{DbManagerService, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.text.MessageFormat
import java.util
import scala.collection.JavaConversions.asScalaBuffer

/**
 * ${description}
 *
 * @author cdxiongmei
 * @version V1.0
 */
object YxyOkrTableInfoUpdate {
  private val LOGGER = LoggerFactory.getLogger(YxyOkrTableInfoUpdate.getClass)
  private val EA_METRICS_TABLE_FIELD = "data_group,data_type,data_sub_type,data,period_start_time,period_end_time,source,create_time,update_time"

  def main(args: Array[String]): Unit = {
    val buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME)
    LOGGER.info("buffaloNtime:" + buffaloNtime)
    val jobTime: DateTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val sparkSession = SparkUtil.buildMergeFileSession("YxyOkrTableInfoUpdate_" + jobTime.toString("yyyy-MM-dd"))
    if (jobTime.getDayOfWeek == NumberConstant.INT_5) {
      /**
       * 每周五需要更新mysql中的周报信息
       */
      dealOkrData(sparkSession, jobTime, false)
    }

  }

  def dealOkrData(sparkSession: SparkSession, jobTime: DateTime, isTest: Boolean): Unit = {
    sparkSession.sql("use ae_public")
    val lastWeekToday = jobTime.minusDays(8).toString("yyyy-MM-dd")
    sparkSession.udf.register("getDataType", (templateName: String) => {
      val dataType = CATemplateEnum.getCATemplateEnum(templateName)
      if (dataType == null) {
        "mixData"
      } else {
        dataType.getDataType
      }
    })
    val valueList = new util.ArrayList[String]
    val tableInfoTemplate: String =
      s"""
         |SELECT
         |		rows             ,
         |		table_name       ,
         |		source_type      ,
         |		if(source_type in (2,3), SPLIT(table_name,"_")[1],null) as template_id,
         |		table_create_time,
         |		table_update_time,
         |		account_id
         |	FROM
         |		adm_yxy_okr_table_info_tb
         |	WHERE
         |		dt = ''{0}''
         |		and source_type in (1,2,3,8)
         |    and account_id not in (10042,10300,10301,10959,10961,11018,10845,11010,10431,10676,10003,10127,10318,10822,10508,10970,11071,11503,10859)
         |""".stripMargin
    val endDate = jobTime.minusDays(2).toString("yyyy-MM-dd")
    val curDay: String = MessageFormat.format(tableInfoTemplate, endDate)
    val curData: Dataset[Row] = sparkSession.sql(curDay)
    curData.cache()
    /**
     * 一周前的快照数据
     */
    val analysisDate: Map[String, String] = Map("increaseUploadCount" -> lastWeekToday,
      "uploadCountTotal" -> "2020-12-31",
      "uploadCountTotal2022" -> "2021-12-31")
    var datadetail: Dataset[Row] = sparkSession.emptyDataFrame
    analysisDate.foreach(analyzerKey => {
      val startDate = analyzerKey._2
      val lastWeekDay: String = MessageFormat.format(tableInfoTemplate, startDate)
      LOGGER.info("curDay:" + curDay + "\n lastWeekDay:" + lastWeekDay)

      /**
       * update_rows
       * data_type
       * data_sub_type
       */
      var curDetail = curData.as("cur_tb").join(sparkSession.sql(lastWeekDay).as("last_week_tb"), Seq("table_name", "account_id"), "left")
        .selectExpr("IF(COALESCE(last_week_tb.rows, 0)=0, cur_tb.rows, if(last_week_tb.rows>cur_tb.rows,cur_tb.rows, cur_tb.rows-last_week_tb.rows)) AS update_rows",
          "getDataType(cur_tb.template_id) as data_type",
          "case when cur_tb.source_type in (2,3) then cur_tb.template_id when cur_tb.source_type=1 then 'diyTableData' when cur_tb.source_type=8 then 'modelEngine' END AS data_sub_type")
      curDetail = curDetail.groupBy("data_type", "data_sub_type")
        .agg(functions.sum("update_rows").as("rows"))
        .withColumn("analysis_key", functions.concat(functions.lit("\\\""), functions.lit(analyzerKey._1), functions.lit("\\\":"), functions.col("rows")))
      if (datadetail.isEmpty) {
        datadetail = curDetail
      } else {
        datadetail = datadetail.union(curDetail)
      }
    })
    /**
     * 需要封装json数据    {"uploadCountTotal":12321, "increaseUploadCount":12321, "uploadCountTotal2022":12321}
     */
    val resultRows: util.List[Row] = datadetail.groupBy("data_type", "data_sub_type")
      .agg(functions.concat_ws(",", functions.collect_set(functions.col("analysis_key"))).as("group_data_origin"))
      .selectExpr("data_type", "data_sub_type", "concat('{',group_data_origin,'}') as group_data").collectAsList()
    resultRows.foreach(
      valueItem => {
        valueList.add(
          s"""
             |(
             |  'dataUploadMetrics',
             |  '${valueItem.getAs("data_type")}',
             |  '${valueItem.getAs("data_sub_type")}',
             |  '${valueItem.getAs("group_data")}',
             |  '${lastWeekToday}',
             |  '${jobTime.minusDays(1).toString("yyyy-MM-dd")}',
             |  'cdp',
             |  '${new DateTime().toString(ConfigProperties.DATEFORMATE_TIMESTAMP)}',
             |  '${new DateTime().toString(ConfigProperties.DATEFORMATE_TIMESTAMP)}'
             |)
             |""".stripMargin)
        if (valueList.size >= ConfigProperties.UPDATE_LIMITNUM) {
          insertMappingSql(valueList, EA_METRICS_TABLE_FIELD, "ea_metrics")
          valueList.clear()
        }
      }
    )
    insertMappingSql(valueList, EA_METRICS_TABLE_FIELD, "ea_metrics")
  }

  /**
   * 更新数据到mysql表中
   *
   * @param insertValueList
   * @param fields
   * @param tableName
   * @param isReplace
   * @param isTest
   * @return
   */
  private def insertMappingSql(insertValueList: util.List[String], fields: String, tableName: String) = {
    var insertMark = "REPLACE INTO ";
    val insertSql = insertMark + tableName + "( " + fields + ")\nVALUES " + StringUtils.join(insertValueList, ",")
    updateSql(insertSql)
  }

  /**
   * 执行DSL
   *
   * @param sql
   */
  private def updateSql(sql: String) = {
    val conn = DbManagerService.getConn
    val stmt = DbManagerService.stmt(conn)
    val deleteRow = DbManagerService.executeUpdate(stmt, sql)
    if (deleteRow > BigInteger.ZERO.intValue) {
      LOGGER.info("数据更新成功rows = {}", deleteRow)
    }
    DbManagerService.close(conn, stmt, null, null)
  }

}
