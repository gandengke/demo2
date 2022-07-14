package com.jd.easy.audience.task.dataintegration.step

import java.util

import com.jd.easy.audience.common.constant.StringConstant
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.task.commonbean.bean.CreateTabelBean
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.SparkUtil
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer
/**
 * 创建hive表
 */
class CreateHiveTableStep extends StepCommon[util.Map[String, AnyRef]]{
  private val LOGGER = LoggerFactory.getLogger(classOf[CreateHiveTableStep])
  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: util.Map[String, StepCommonBean[_]]): util.Map[String, AnyRef] = {
    val bean: CreateTabelBean = getStepBean.asInstanceOf[CreateTabelBean]
    LOGGER.info("tableCreateSql：" + bean.getTableCreateSql.stripMargin)
    val sparkSession = SparkUtil.buildMergeFileSession(bean.getStepName);
    val dbName: String = bean.getDbName
    val targetTable: String = bean.getTargetTable
    val existDrop: Boolean = bean.getExistDrop
    //1. 元数据信息
    if(!sparkSession.catalog.databaseExists(dbName)) {
      //1.1 库不存在
      throw new JobInterruptedException(s"""$dbName 库不存在，请重新检查""", s"Failure: The database $dbName is not exist!")
    }
    if (sparkSession.catalog.tableExists(s"""$dbName.$targetTable""") && !existDrop) {
      //1.2 表存在且未指定删除
      throw new JobInterruptedException(s"""$dbName.$targetTable 表不存在，请重新检查""", "Failure: The Table " + dbName + "." + targetTable + " is not exist!")
    } else if(sparkSession.catalog.tableExists(s"""$dbName.$targetTable""") && existDrop){
      SparkUtil.printAndExecuteSql(s"""drop table $dbName.$targetTable""", sparkSession)
    }
    try {
      SparkUtil.printAndExecuteSql(bean.getTableCreateSql, sparkSession)
    } catch  {
      case e: Exception =>
        LOGGER.info("建表异常", e)
        throw new JobInterruptedException(s"""$dbName.$targetTable 表创建失败，请重新检查建表语句""", s"""Failure: Create Table $dbName.$targetTable failed!""")
    }
    //创建成功后再次检查表
    val metaInfo = sparkSession.catalog.listColumns(s"""$dbName.$targetTable""")
    val colCount = metaInfo.count()
    LOGGER.info(s"""$dbName.$targetTable 表创建成功，字段数$colCount""")
    //查看元数据信息
    var resultMap = new util.HashMap[String, Object]()
    var partitionColsList: java.util.List[String] = new java.util.ArrayList[String]()
    sparkSession.catalog.listColumns(bean.getDbName, bean.getTargetTable).filter(ConfigProperties.FILTER_PATITION_FIELD).collectAsList().foreach(partitionCol => {
      partitionColsList.add(partitionCol.name)
    })
    //元数据信息返回
    resultMap.put("fileSize", "0.00")
    resultMap.put("rowNumber", StringConstant.ZERO)
    resultMap.put("modifiedtime", "")
    resultMap.put("partitionKey", StringUtils.join(partitionColsList, StringConstant.COMMA))
    bean.setOutputMap(resultMap)
    resultMap
  }
}