package com.jd.easy.audience.task.dataintegration.step

import java.math.BigInteger
import java.sql.ResultSet
import java.text.{MessageFormat, SimpleDateFormat}
import java.util
import java.util.Date

import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.task.commonbean.bean.CataUpdatelBean
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.TableMetaUtil.getLocalUri
import com.jd.easy.audience.task.dataintegration.util.{DbManagerService, SparkUtil, TableMetaInfo, TableMetaUtil}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 租户维度更新表的元数据信息-资源管控更新元数据信息
 */
class CataTableInfoUpdateTenantStep extends StepCommon[Unit] {
  private val LOGGER = LoggerFactory.getLogger(classOf[CataTableInfoUpdateTenantStep])
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val UPDATE_LIMITNUM: Int = NumberConstant.INT_40

  override
  def run(dependencies: util.Map[String, StepCommonBean[_]]): Unit = {
    //1、 基础信息获取
    val bean: CataUpdatelBean = getStepBean.asInstanceOf[CataUpdatelBean]
    LOGGER.info(System.getenv().toString)
    implicit val proAccountName: String = System.getenv(ConfigProperties.HADOOP_USER) //生产账号
    implicit val dbName: String = bean.getDbName //数据库
    implicit val accountId: Long = bean.getAccountId //组织id
    implicit val accountName: String = bean.getAccountName //组织名哼
    implicit val sparkSession = SparkUtil.buildMergeFileSession("CataTableInfoUpdateTenantStep_" + bean.getAccountId);
    //2. mysql中获取数据源表信息
    val mysqlTbInfos = getTableData(sparkSession, proAccountName, dbName, accountId)
    //3. 获取数据库信息列表,设置广播变量
    synchronizeMysqlTableInfo(mysqlTbInfos, Tuple3(dbName, accountId, accountName), sparkSession)
    //5. 清理现场
  }

  /**
   * 获取对应生产账号mysql中的建表信息
   *
   * @param sparkSession
   * @param proAccountName
   * @return
   */
  def getTableData(implicit sparkSession: SparkSession, proAccountName: String, dbName: String, accountId: Long): Dataset[Row] = {
    val templateConfig = ConfigProperties.templateInfo.getConfig(this.getClass.getSimpleName).getConfig(new Exception().getStackTrace()(0).getMethodName())
    val fieldNames = templateConfig.getString("fieldNames")
    val schema =
      StructType(
        fieldNames.split(StringConstant.COMMA).map(fieldName => StructField(fieldName, StringType, true)))
    val template = templateConfig.getString("searchTemplate");
    val searchSql: String = MessageFormat.format(template, proAccountName, accountId.toString, dbName)
    val jdbcOp = new JdbcOperate()
    val rowData: java.util.List[Row] = jdbcOp.searchTableInfo(searchSql)
    sparkSession.createDataFrame(rowData, schema)
  }

  /**
   * 更新hive数据库的表信息到mysql中
   *
   * @param dbMysqltbInfo mysql中的建表信息
   * @param accountInfos  库和组织id关联关系
   * @param sparkSession
   */
  def synchronizeMysqlTableInfo(dbMysqltbInfo: Dataset[Row], accountInfos: Tuple3[String, Long, String], sparkSession: SparkSession): Unit = {
    //数据更新时间
    val updateTime: java.util.Date = new Date
    LOGGER.info(s"dbname=${accountInfos._1}, accountId=${accountInfos._2}, accountName=${accountInfos._3},updateTime=${updateTime.toLocaleString} start deal tables")
    //1. hive中读取对应库的所有列表信息进行封装
    val hiveTbInfos: Dataset[Row] = sparkSession.catalog.listTables(accountInfos._1).mapPartitions {
      iter => {
        var result = new ArrayBuffer[Row]()
        while (iter.hasNext) {
          val row = iter.next()
          result += Row.apply(row.database, row.name, row.description)
        }
        result.iterator
      }
    }(RowEncoder(
      StructType(
        Seq(
          StructField(ConfigProperties.FIELD_DATABASE_NAME, StringType),
          StructField(ConfigProperties.FIELD_TABLE_NAME, StringType),
          StructField(ConfigProperties.FIELD_TABLE_COMMENT_NEW, StringType)))))

    //2. mysql中的表和hive中的表进行关联
    val joinData = hiveTbInfos.join(dbMysqltbInfo, Seq(ConfigProperties.FIELD_DATABASE_NAME, ConfigProperties.FIELD_TABLE_NAME), StringConstant.LEFT_OUTER)

    //3. 过滤需要新增的数据
    //    val insertData: Dataset[Row] = joinData.filter("id is null").select("database_name", "table_name", "table_comment_new")
    //    formatInsertSql4NewTable(insertData, accountInfos, updateTime, sparkSession)
    //4. 过滤需要更新的数据
    val updateData: Dataset[Row] = joinData.filter(ConfigProperties.FILTER_ID_NOT_NULL)
    val noUpdateIdsList = formatUpdateSql4HisTable(updateData, accountInfos, updateTime, sparkSession)
    //5. 删除数据
    formatDeleteSql4ExpriedTable(noUpdateIdsList, updateTime, accountInfos.getDbName, accountInfos.getAccountId())
  }

  /**
   * 对过期数据进行删除
   *
   * @param noupdateIds
   * @param updateTime
   * @param dbName
   */
  def formatDeleteSql4ExpriedTable(noupdateIds: java.util.List[Long], updateTime: Date, dbName: String, accountId: Long): Unit = {
    LOGGER.info("删除元数据信息update_time!=" + SIMPLE_DATE_FORMAT.format(updateTime))
    val templateConfig = ConfigProperties.templateInfo.getConfig(this.getClass.getSimpleName).getConfig(new Exception().getStackTrace()(0).getMethodName())
    var querySql = MessageFormat.format(templateConfig.getString("deleteExpriedTemplate"), dbName, accountId.toString, SIMPLE_DATE_FORMAT.format(updateTime))
    if (!CollectionUtils.isEmpty(noupdateIds)) {
      querySql = querySql + " and id not in (" + StringUtils.join(noupdateIds, StringConstant.COMMA) + ")"
    }
    LOGGER.info("selectIds sql=" + querySql)
    val jdbcOp = new JdbcOperate()
    val retIds: java.util.List[Row] = jdbcOp.searchIdInfo(querySql)
    val idList: java.util.List[String] = retIds.map(row => row.getAs[String](NumberConstant.INT_0)).toList
    if (idList.length > NumberConstant.INT_0) {
      val deleteSql = MessageFormat.format(templateConfig.getString("updateTemplate"), SIMPLE_DATE_FORMAT.format(updateTime), StringUtils.join(idList, StringConstant.COMMA))
      val effectRows = jdbcOp.update(deleteSql)
      if (effectRows > NumberConstant.INT_0) {
        LOGGER.info("数据影响行rows={}", effectRows)
      }
    }
  }

  private def getModifiedInfo(dbName: String, tbName: String, sparkSession: SparkSession): TableMetaInfo = {
    val tableLocation = getLocalUri(dbName, tbName, sparkSession)
    val path = new Path(tableLocation)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val (modifiedTime, fileSize) = TableMetaUtil.getModificationTime(path, fileSystem)
    var hiveModifiedTime = StringConstant.EMPTY
    if (modifiedTime > NumberConstant.INT_0) {
      hiveModifiedTime = SIMPLE_DATE_FORMAT.format(modifiedTime)
    }
    return new TableMetaInfo(modifiedTime, fileSize, NumberConstant.INT_0, null)
  }

  /**
   * 封装更新表的mysql语句
   *
   * @param dataset
   * @param accountInfo
   * @param updateTime
   * @param sparkSession
   * @return
   */
  def formatUpdateSql4HisTable(dataset: Dataset[Row], accountInfo: Tuple3[String, Long, String], updateTime: Date, sparkSession: SparkSession): java.util.List[Long] = {
    // 1.初始化信息
    val bean: CataUpdatelBean = getStepBean.asInstanceOf[CataUpdatelBean]
    val updatevalueList: java.util.List[String] = new java.util.ArrayList[String]()
    val noUpdateIdsList: java.util.List[Long] = new java.util.ArrayList[Long]()
    val templateConfig = ConfigProperties.templateInfo.getConfig(this.getClass.getSimpleName).getConfig(new Exception().getStackTrace()(0).getMethodName())
    val valueUpateFormat = templateConfig.getString("valueUpdate")

    dataset.collect().foreach(rowItem => {
      val tableName: String = rowItem.getAs[String](ConfigProperties.FIELD_TABLE_NAME)
      breakable {
        // 2.1. 表是否在hive中存在
        if (!sparkSession.catalog.tableExists(accountInfo.getDbName(), tableName)) {
          LOGGER.info(s"${accountInfo.getDbName()}.${tableName} is not exist")
          break
        }
        // 2.2. 元数据信息获取
        val fileInfo: TableMetaInfo = getModifiedInfo(accountInfo.getDbName(), tableName, sparkSession)
        val hiveModifiedTime: String = fileInfo.getmodificationTimeFormat()
        var mysqlModifiedTime = StringConstant.EMPTY
        if (StringUtils.isNotBlank(rowItem.getAs[String](ConfigProperties.FIELD_TABLE_DATA_UPDATE_TIME))) {
          mysqlModifiedTime = rowItem.getAs[String](ConfigProperties.FIELD_TABLE_DATA_UPDATE_TIME).substring(NumberConstant.INT_0, NumberConstant.INT_19)
        }
        LOGGER.info("mysql modifiedTime=" + mysqlModifiedTime + ", dataModifiedTime=" + hiveModifiedTime)

        // 3.判断是否需要更新
        if (!hiveModifiedTime.equalsIgnoreCase(mysqlModifiedTime) || bean.getIsAllUpdate) {
          // 3.1 需要更新元数据
          val (rowNumber, partitionList) = TableMetaUtil.getCataNumRows(accountInfo.getDbName, tableName, sparkSession)
          var tablecomment = StringConstant.EMPTY
          if (StringUtils.isNotBlank(rowItem.getAs[String](ConfigProperties.FIELD_TABLE_COMMENT_NEW))) {
            tablecomment = rowItem.getAs[String](ConfigProperties.FIELD_TABLE_COMMENT_NEW)
          }
          val updateValueStr = MessageFormat.format(valueUpateFormat,
            rowItem.getAs[String](ConfigProperties.FIELD_ID),
            rowItem.getAs[String](ConfigProperties.FIELD_ACCOUNT_ID),
            rowItem.getAs[String](ConfigProperties.FIELD_MAIN_ACCOUNT_NAME),
            rowItem.getAs[String](ConfigProperties.FIELD_SUB_ACCOUNT_NAME),
            rowItem.getAs[String](ConfigProperties.FIELD_DATABASE_NAME),
            tableName,
            tablecomment,
            fileInfo.getfileSizeFormat(),
            if (StringUtils.isBlank(hiveModifiedTime)) null else "\"" + hiveModifiedTime + "\"",
            SIMPLE_DATE_FORMAT.format(updateTime),
            rowNumber.toString,
            StringUtils.join(partitionList, StringConstant.COMMA)
          )
          updatevalueList.add(updateValueStr)
          // 3.1.1分批进行处理
          if (updatevalueList.size() >= UPDATE_LIMITNUM) {
            updateCata2Mysql(updatevalueList, updateTime)
            updatevalueList.clear()
          }
        } else {
          // 3.2 无任何更新,需要在删除数据的时候排除
          noUpdateIdsList.add(rowItem.getAs[String](ConfigProperties.FIELD_ID).toLong)
        }
      }
    })
    // 4. 处理剩余更新记录
    if (updatevalueList.size() > NumberConstant.INT_0) {
      updateCata2Mysql(updatevalueList, updateTime)
    }
    noUpdateIdsList
  }

  /**
   * @description 更新信息sql拼接
   * @param [updatevalueList, updateTime]
   * @return int
   * @date 2022/1/6 下午2:12
   * @auther cdxiongmei
   */
  def updateCata2Mysql(updatevalueList: java.util.List[String], updateTime: Date): Int = {
    val templateConfig = ConfigProperties.templateInfo.getConfig(this.getClass.getSimpleName).getConfig(new Exception().getStackTrace()(0).getMethodName())
    val updateSql = MessageFormat.format(templateConfig.getString("updateTemplate"), StringUtils.join(updatevalueList, StringConstant.COMMA))
    LOGGER.info("update update_time=" + SIMPLE_DATE_FORMAT.format(updateTime));
    val jdbcOp = new JdbcOperate()
    val updateRow: Int = jdbcOp.update(updateSql)
    if (updateRow > BigInteger.ZERO.intValue) {
      LOGGER.info("数据更新成功rows={}", updateRow)
    }
    return updateRow
  }

  /**
   * 定义隐式转换
   *
   * @param accountInfos
   */
  implicit class AccountInfo(accountInfos: Tuple3[String, Long, String]) {
    def getDbName(): String = {
      accountInfos._1
    }

    def getAccountId(): Long = {
      accountInfos._2.toLong
    }

    def getAccountName(): String = {
      accountInfos._3
    }
  }

  class JdbcOperate {
    def update(updateSql: String): Int = {
      val conn = DbManagerService.getConn
      val stmt = DbManagerService.stmt(conn)
      try {
        val rows: Int = DbManagerService.executeUpdate(stmt, updateSql)
        return rows
      } catch {
        case ex: Exception => throw ex
      }
      finally {
        DbManagerService.close(conn, stmt, null, null)
      }
      0
    }

    def searchTableInfo(searchSql: String): java.util.List[Row] = {
      val conn = DbManagerService.getConn
      val stmt = DbManagerService.stmt(conn)
      var retResult: ResultSet = null
      try {
        retResult = DbManagerService.executeQuery(stmt, searchSql);
        var rowData: java.util.List[Row] = new java.util.ArrayList[Row]()
        if (null == retResult) {
          LOGGER.info("数据影响行rows=0")
          return rowData
        }
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
        }
        return rowData
      } catch {
        case ex: Exception => {
          throw ex
        }
      } finally {
        DbManagerService.close(conn, stmt, null, retResult)
      }
    }

    def searchIdInfo(searchSql: String): java.util.List[Row] = {
      val conn = DbManagerService.getConn
      val stmt = DbManagerService.stmt(conn)
      var retResult: ResultSet = null
      try {
        retResult = DbManagerService.executeQuery(stmt, searchSql);
        var rowData: java.util.List[Row] = new java.util.ArrayList[Row]()
        if (null == retResult) {
          LOGGER.info("数据影响行rows=0")
          return rowData
        }
        while (retResult.next()) {
          rowData.add(RowFactory.create(
            retResult.getLong("id").toString
          ))
        }
        return rowData
      } catch {
        case ex: Exception => throw ex
      } finally {
        DbManagerService.close(conn, stmt, null, retResult)
      }
    }
  }

}
