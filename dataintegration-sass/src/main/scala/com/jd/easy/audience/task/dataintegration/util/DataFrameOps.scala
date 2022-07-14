package com.jd.easy.audience.task.dataintegration.util

import java.util.regex.Pattern

import com.jd.easy.audience.common.exception.JobInterruptedException
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random


object DataFrameOps {

  val LOGGER: Logger = LoggerFactory.getLogger("DataFrameOps")
  /**
   * 正则
   */
  private val linePattern = Pattern.compile("_(\\w)")

  implicit class DataFrameOps(df: DataFrame) {
    def saveToHive(tableName: String, isHump: Boolean): Unit = {
      val spark = df.sparkSession
      val viewName = "spark_temp_view_" + Random.nextInt(1000)
      df.createOrReplaceTempView(viewName)
      val dataColumns = df.columns
      val (dataCols, partitionCols) = getColumns(tableName, spark)
      //非分区字段处理
      var existColumns = dataCols.map(c => {
        var fieldDf: String = c
        if (isHump) {
          fieldDf = lineToHump(c)
        }
        if (dataColumns.contains(fieldDf)) s"$fieldDf as ${c}" else s"null as $c"
      })
      //分区字段处理
      var partitionDynamic: Array[String] = Array()
      var partitionDesc: Array[String] = Array()
      partitionCols.foreach(par => {
        var fieldDf: String = par
        if (isHump) {
          fieldDf = lineToHump(par)
        }
        if (dataColumns.contains(fieldDf)) {
          //动态分区
          existColumns = existColumns :+ (s"$fieldDf as ${par}")
          partitionDynamic = partitionDynamic :+ par
          partitionDesc = partitionDesc :+ par
        } else if (partitionDynamic.length > 0) {
          //动态分区字段出现在静态分区字段前
          LOGGER.error("动态分区字段不能出现在静态分区字段前")
          throw new JobInterruptedException("动态分区字段不能出现在静态分区字段前")
        } else {
          val dateStr = DateTime.now().toString("yyyy-MM-dd")
          partitionDesc = partitionDesc :+ s"$par='" + dateStr + "'"
        }
      })
      val partitionRule =
        if (!partitionDesc.isEmpty) s"partition(${partitionDesc.mkString(",")})" else ""
      var insertSql =
        s"""
           |insert into table $tableName $partitionRule
           |select ${existColumns.mkString(",")} from $viewName
           """.stripMargin
      LOGGER.info(s"insert sql: $insertSql, size: " + df.count())
      spark.sql(insertSql)
    }
  }

  /**
   * 将驼峰转下划线格式
   *
   * @param str
   * @return
   */
  private def humpToLine(str: String): String = str.replaceAll("[A-Z]", "_$0").toLowerCase

  /**
   * 下划线转驼峰
   *
   * @param str
   * @return
   */
  def lineToHump(str: String): String = {
    val str_new = str.toLowerCase
    val matcher = linePattern.matcher(str_new)
    val sb = new StringBuffer
    while ( {
      matcher.find
    }) matcher.appendReplacement(sb, matcher.group(1).toUpperCase)
    matcher.appendTail(sb)
    sb.toString
  }

  /**
   * 获取非分区字段和分区字段
   *
   * @param tableName
   * @return
   */
  def getColumns(tableName: String, spark: SparkSession): (Array[String], Array[String]) = {
//    spark.sql("show databases").show()
//    LOGGER.info("getColumns={}", spark.catalog.listDatabases().select("name").as(Encoders.STRING).collectAsList().toString)
    val metaInfo = spark.catalog.listColumns(tableName).cache()
    (
      metaInfo.filter(s"isPartition = 'false'").select("name").as(Encoders.STRING).collect(),
      metaInfo.filter(s"isPartition = 'true'").select("name").as(Encoders.STRING).collect()
    )
  }
}
