package com.jd.easy.audience.task.plugin.run.spark

import com.fasterxml.jackson.annotation.JsonTypeName
import org.slf4j.LoggerFactory

/**
 * @title CataTableInfoStep
 * @description 创建hive表
 * @author cdxiongmei
 * @date 2021/8/10 上午10:32
 * @throws
 */
@deprecated
@JsonTypeName(value = "cataTableInfoStep")
object CataTableInfoStep {
  private val LOGGER = LoggerFactory.getLogger(CataTableInfoStep.getClass)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkLauncherUtil.buildSparkSession("CataTableInfoStep");
    //1. 元数据信息
    LOGGER.info("===================show database===================")
    sparkSession.catalog.listDatabases().collect.foreach(db => {
      val dbName = db.name
      val des = db.description
      val localUri = db.locationUri
      LOGGER.info("dbName:" + dbName + ",des=" + des + ",localUri=" + localUri)
      sparkSession.catalog.listTables(dbName).collect().map(tb => {
        val tbName = tb.name
        val tbdes = tb.description
        val tableType = tb.tableType
        LOGGER.info("tbName:" + tbName + ",des=" + tbdes + ",tableType=" + tableType)
        sparkSession.catalog.listColumns(dbName, tbName).collect().map(column =>{
          val colName = column.name
          val colDes = column.description
          val dataType = column.dataType
          val isPartition = column.isPartition
          LOGGER.info("colName:" + colName + ",colDes=" + colDes + ",dataType=" + dataType + ",isPartition=" + isPartition)
        })
      })
    })
    //元数据信息返回
    val resultMap = new java.util.HashMap[String, Object]()
    LOGGER.info("resultMap=" + resultMap.toString)
  }



}