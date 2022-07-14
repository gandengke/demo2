package com.jd.easy.audience.task.dataintegration.run.spark

import java.math.BigInteger
import java.sql.{ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale}

import com.jd.easy.audience.common.constant.NumberConstant
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.parsing.json._

/**
 * 更新表的前缀信息
 */
@Deprecated
object CataTableInfoUpdatePrefixStep {
  private val LOGGER = LoggerFactory.getLogger(CataTableInfoUpdatePrefixStep.getClass)
  private val CATATIME_FROMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH) //多态
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    //1. 元数据信息
    val now = SIMPLE_DATE_FORMAT.format(new Date)
    LOGGER.info("CataTableInfoUpdateStepNew start！update_time={}", now)
    val sparkSession = SparkUtil.buildMergeFileSession("CataTableInfoUpdatePrefixStep")
    val conn = DbManagerService.getConn
    val stmt = DbManagerService.stmt(conn)
    //2. mysql中获取数据源表信息
    var mysqlTbInfos = getTableData(stmt, sparkSession).filter("status=0 and yn=1")
    //3. mysql中获取账号和库相关信息
    val dbInfoList = TableMetaUtil.getJnosDatabaseInfo()
    //4. 对库进行循环更新表名
    val dbName = args(0)
    LOGGER.info("test dbName={}", dbName)
    breakable {
      dbInfoList.foreach(dbInfo => {
        if (dbInfo.getDbName.equalsIgnoreCase(dbName)) {
          LOGGER.info("deal database={}", dbInfo.getDbName)
          val dbMysqltbInfo = mysqlTbInfos.filter("database_name=\"" + dbInfo.getDbName + "\"")
          dbMysqltbInfo.show(NumberConstant.INT_10)
          synchronizeMysqlTableInfo(dbMysqltbInfo, dbInfo, now, sparkSession, stmt)
          break
        }
      })
    }
    DbManagerService.close(conn, stmt, null, null)
  }

  /**
   * 获取mysql中的建表信息
   *
   * @param stmt
   * @param spark
   * @return
   */
  def getTableData(stmt: Statement, spark: SparkSession): Dataset[Row] = {
    val fieldNames = "id,account_id,main_account_name,sub_account_name,database_name,table_name,table_comment,table_storage_size,table_data_update_time,status,yn,create_time,table_row_count"
    val schema =
      StructType(
        fieldNames.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val searchSql: String = "SELECT " + fieldNames + " FROM " + ConfigProperties.TABLE_INFO_TB
    val retResult: ResultSet = DbManagerService.executeQuery(stmt, searchSql);
    var rowData: util.List[Row] = new util.ArrayList[Row]()
    while (retResult.next()) {
      rowData.add(RowFactory.create(
        retResult.getLong("id").toString,
        retResult.getLong("account_id").toString,
        retResult.getString("main_account_name"),
        retResult.getString("sub_account_name"),
        retResult.getString("database_name"),
        retResult.getString("table_name"),
        retResult.getString("table_comment"),
        retResult.getString("table_storage_size"),
        retResult.getString("table_data_update_time"),
        retResult.getInt("status").toString,
        retResult.getInt("yn").toString,
        retResult.getString("create_time"),
        retResult.getLong("table_row_count").toString
      ))
    } //显示数据
    spark.createDataFrame(rowData, schema)
  }

  /**
   * 更新hive数据库的表信息到mysql中
   *
   * @param dbMysqltbInfo mysql中的建表信息
   * @param relInfo       库和组织id关联关系
   * @param updateTime    数据更新时间
   * @param sparkSession
   */
  def synchronizeMysqlTableInfo(dbMysqltbInfo: Dataset[Row], relInfo: JnosRelInfo, updateTime: String, sparkSession: SparkSession, stmt: Statement): Unit = {
    LOGGER.info(s"dbname=${relInfo.getDbName}, accountId=${relInfo.getAccountId} start deal tables")
    //5. 过滤需要更新的数据
    val updateData: Dataset[Row] = dbMysqltbInfo.select("id", "database_name", "table_name")
    formatUpdateSql4HisTable(updateData, relInfo, updateTime, sparkSession, stmt)
  }
  /**
   * 封装更新表的mysql语句
   *
   * @param dataset
   * @param relInfo
   * @param updateTime
   * @param sparkSession
   * @return
   */
  def formatUpdateSql4HisTable(dataset: Dataset[Row], relInfo: JnosRelInfo, updateTime: String, sparkSession: SparkSession, stmt: Statement): Unit = {

    val ranameTbList: java.util.List[(String)] = new util.ArrayList[(String)]()
    val updateIdsList: java.util.List[Long] = new util.ArrayList[Long]()
    //"id", "database_name", "table_name",
    dataset.collect().foreach(rowItem => {
      breakable {
        val tableNameOld: String = rowItem.getAs[String]("table_name")
        LOGGER.info("更新元数据信息table_name=" + tableNameOld)
        var tableNameNew: String = tableNameOld
        if (!sparkSession.catalog.tableExists(relInfo.getDbName, tableNameOld) || (
          !tableNameOld.startsWith(ConfigProperties.CDP_TABLE_PREFIX) &&
            sparkSession.catalog.tableExists(relInfo.getDbName, "adm_" + tableNameOld)
          )) {
          //如果表不存在
          val updateSql2 = "update " + ConfigProperties.TABLE_INFO_TB + " set `yn`= 0" +
            " where id =" + rowItem.getAs[String]("id").toLong
          val updateRow2 = DbManagerService.executeUpdate(stmt, updateSql2)
          if (updateRow2 > BigInteger.ZERO.intValue) {
            LOGGER.info("数据更新成功yn=0 ,rows={}", updateRow2)
          }

        } else if (!tableNameOld.startsWith(ConfigProperties.CDP_TABLE_PREFIX)) {
          //1. 存量的表不是以adm前缀开始的表
          sparkSession.sql(s"alter table ${relInfo.getDbName}.$tableNameOld rename to ${relInfo.getDbName}.adm_$tableNameOld")
          tableNameNew = s"adm_$tableNameOld"
          LOGGER.info(s"更新表名前缀，tableNameOld=$tableNameOld，tableNameNew =$tableNameNew")
          ranameTbList.add((tableNameOld))
          updateIdsList.add(rowItem.getAs[String]("id").toLong)
        }
      }
    })
    //更新表名
    if (updateIdsList.size() > 0) {
      LOGGER.info("update update_time=" + updateTime);
      val updateSql = "update " + ConfigProperties.TABLE_INFO_TB + " set `table_name`= concat(\"adm_\", `table_name`)" +
        " where id in (" + StringUtils.join(updateIdsList, ",") + ")"
      val updateRow = DbManagerService.executeUpdate(stmt, updateSql)
      if (updateRow > BigInteger.ZERO.intValue) {
        LOGGER.info("数据更新成功rows={}", updateRow)
      }
    }
    //更新数据集信息
    if (!ranameTbList.isEmpty) {
      LOGGER.info("需要更新关联数据集")
      //标签数据集
      var datasetUpdate = "update " +
        " ea_dataset dt " +
        "set `userId` =concat(replace(`userId`, '" + relInfo.getDbName + ".', '" + relInfo.getDbName + ".adm_'))" +
        "  where type in ('tag','rfm')  and dt.yn=1 and dt.mainAccountName =\"" + relInfo.getMainAccountName + "\" and userId  REGEXP '^" +
        relInfo.getDbName +
        s".(${StringUtils.join(ranameTbList, '|')}).'"
      LOGGER.info("datasetUpdate=" + datasetUpdate)
      val retDataset = DbManagerService.executeUpdate(stmt, datasetUpdate);
      LOGGER.info("retDataset ret=" + retDataset)
      //标签明细表
      var labelUpdate = "update " +
        " ea_dataset_label_detail dt " +
        "set `tableName` =concat(\"adm_\", `tableName`)" +
        "  where  dt.yn=1 and dt.mainAccountName =\"" + relInfo.getMainAccountName + "\" and `tableName` in " +
        "(\"" + StringUtils.join(ranameTbList, "\",\"") + "\")"
      LOGGER.info("labelUpdate=" + labelUpdate)
      val retLabel = DbManagerService.executeUpdate(stmt, labelUpdate);
      LOGGER.info("retLabel ret=" + retLabel)
      //rfm数据更新
      var rfmDatasetInfo = "update " +
        "ea_dataset_rfm_detail tb " +
        "inner join ea_dataset dt " +
        "on dt.id = tb.dataSetId " +
        "set  `tableName` =concat(\"adm_\", `tableName`)" +
        "  where type ='rfm' and tb.yn=1 and dt.yn=1 and dt.mainAccountName =\"" + relInfo.getMainAccountName + "\" and `tableName` in " +
        "(\"" + StringUtils.join(ranameTbList, "\",\"") + "\")"
      LOGGER.info("rfmUpdate=" + rfmDatasetInfo)
      val retRfm = DbManagerService.executeUpdate(stmt, rfmDatasetInfo);
      LOGGER.info("retRfm=" + retRfm)
      //4a数据更新
      ranameTbList.foreach(tbName => {
        //rfm数据集更新
        val rfmData: String = "SELECT id," +
          "json_search(frontJson,'all','" + tbName + "'), " +
          "Json_extract(frontJson, '$.core') " +
          "FROM  ea_rfm_dataset_info WHERE\n  " +
          s"Json_contains(\n    json_object(\n      'key',\n " +
          " Json_extract(frontJson -> '$.cardGroup', '$[*].dataTableName')\n    ),\n    json_array('" + tbName + "'),\n    '$.key'  )"
        val rfmRetInfo = DbManagerService.executeQuery(stmt, rfmData)
        var rfmSetInfoTupe: Array[(Long, String, String)] = Array()
        while (rfmRetInfo.next()) {
          LOGGER.info(rfmRetInfo.toString)
          rfmSetInfoTupe = rfmSetInfoTupe :+ (rfmRetInfo.getLong(1), rfmRetInfo.getString(2), rfmRetInfo.getString(3))
        }
        LOGGER.info("rfmSetInfoTupe={}", rfmSetInfoTupe.size)
        DbManagerService.close(null, null, null, rfmRetInfo)
        rfmSetInfoTupe.foreach(rfmSetInfoTupe => {
          var frontJsonPosSql: util.List[String] = new util.ArrayList[String]()
          val frontJsonPos = rfmSetInfoTupe._2
          if (StringUtils.isNotBlank(frontJsonPos)) {
            val rfmPosNew = frontJsonPos.substring(1, frontJsonPos.length - 1).replaceAll("\"", "")
            LOGGER.info("rfmPosNew={}", rfmPosNew)
            rfmPosNew.split(",").foreach(tablePos => {
              if (tablePos.contains("dataTableCode")) {
                frontJsonPosSql.add("'" + tablePos.replace("dataTableCode", "key") + "',replace(replace(frontJson->'" + tablePos.replace("dataTableCode", "key") + s"""','$tbName', 'adm_$tbName'),'"','')""")
              }
              frontJsonPosSql.add("\"" + tablePos + "\",\"adm_" + tbName + "\"")
            })
          }
          //core
          val coreInfoPos2 = CustomSerializeUtils.regJson(JSON.parseFull(rfmSetInfoTupe._3))
          LOGGER.info("core json={}", coreInfoPos2.toString())
          coreInfoPos2.keys.foreach(key => {
            LOGGER.info("core json key={}", key)
            LOGGER.info("core json value={}", coreInfoPos2.get(key).toString)
            if (coreInfoPos2.get(key).toString.contains(s".${relInfo.getDbName}.${tbName}.")) {
              frontJsonPosSql.add("'$.core." + key + "',replace(replace(frontJson->'$.core." + key + s"""','.${relInfo.getDbName}.$tbName.', '.${relInfo.getDbName}.adm_$tbName.'),'"','')""")
            }
          })
          var configField = ""
          if (!frontJsonPosSql.isEmpty) {
            configField = "`frontJson` = json_replace(`frontJson`," + StringUtils.join(frontJsonPosSql, ",") + ")"
          }
          val rfmInfoDataUpdate = "UPDATE " +
            "ea_rfm_dataset_info \n" +
            s"set `dateField` =replace(`dateField`, '${relInfo.getDbName}.${tbName}.','${relInfo.getDbName}.adm_${tbName}.')," +
            s"`timesField` =replace(`timesField`, '${relInfo.getDbName}.${tbName}.','${relInfo.getDbName}.adm_${tbName}.')," +
            s"`amountField` =replace(`amountField`, '${relInfo.getDbName}.${tbName}.','${relInfo.getDbName}.adm_${tbName}.')," +
            configField +
            " \nWHERE\n\tid =" + rfmSetInfoTupe._1
          LOGGER.info("rfmInfoDataUpdate={}", rfmInfoDataUpdate)

          val retrfmDataFirst = DbManagerService.executeUpdate(stmt, rfmInfoDataUpdate);
          LOGGER.info("retrfmDataFirst=" + retrfmDataFirst)
        })

        //4a数据集更新
        val uddSearch: String = "SELECT id," +
          s"json_search(rel_dataset,'one','${tbName}')," +
          s"json_search(feature_mapping,'all','${tbName}')" +
          "FROM ea_dataset_4a_detail where " +
          "Json_contains(json_object('key', Json_extract( rel_dataset -> '$.data', '$[*].dataTable' )), json_array('" + tbName + "' ), '$.key')"
        val uddretIds = DbManagerService.executeQuery(stmt, uddSearch)
        var uddSet: Array[(Long, String, String)] = Array()
        while (uddretIds.next()) {
          LOGGER.info(uddretIds.toString)
          uddSet = uddSet :+ (uddretIds.getLong(1), uddretIds.getString(2), uddretIds.getString(3))
        }
        LOGGER.info("uddSetsize={}", uddSet.size)
        DbManagerService.close(null, null, null, uddretIds)
        uddSet.foreach(uddInfo => {
          LOGGER.info("udd name position={}", uddInfo._2.replaceAll("\"", ""))
          var featureSetList: util.List[String] = new util.ArrayList[String]()
          var featureSetSql: String = ""
          val featurePos = uddInfo._3
          if (StringUtils.isNotBlank(featurePos)) {
            val featurePosNew = featurePos.substring(1, featurePos.length - 1).replaceAll("\"", "")
            LOGGER.info("featurePsInfos={}", featurePosNew)
            featurePosNew.split(",").foreach(tablePos => {
              featureSetList.add("\"" + tablePos + "\",\"adm_" + tbName + "\"")
            })
          }
          if (!featureSetList.isEmpty) {
            featureSetSql = ",feature_mapping = json_replace(feature_mapping," + StringUtils.join(featureSetList, ",") + ")"
          }
          val uddFirstPosDataUpdate = "UPDATE " +
            "ea_dataset_4a_detail \n" +
            "SET rel_dataset = json_replace(rel_dataset,\"" + uddInfo._2.replaceAll("\"", "") + "\",\"adm_" + tbName + "\")" +
            featureSetSql +
            " \nWHERE\n\tid =" + uddInfo._1
          LOGGER.info("firstPos={}", uddFirstPosDataUpdate)

          val retUddFirst = DbManagerService.executeUpdate(stmt, uddFirstPosDataUpdate);
          LOGGER.info("retUddFirst=" + retUddFirst)
        })
      })

    }
  }
}