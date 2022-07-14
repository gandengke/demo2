package com.jd.easy.audience.task.dataintegration.util

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 *
 * @author cdxiongmei
 * @version V1.0
 */
object SparkUtil {

  def buildMergeFileSession(appName: String): SparkSession = {
    val sparkSession: SparkSession = new SparkSession.Builder()
      .master("yarn")
      .appName(appName)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.hive.mergeFiles", "true")
      .config("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .config("spark.hadoop.hive.hadoop.supports.splittable.combineinputformat", "true")
      .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256000000")
      .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.node", "256000000")
      .config("spark.hadoop.hive.exec.max.dynamic.partitions", "2000")
      .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.rack", "256000000")
      .config("spark.hadoop.hive.merge.mapfiles", "true")
      .config("spark.hadoop.hive.merge.mapredfiles", "true")
      .config("spark.hadoop.hive.merge.size.per.task", "256000000")
      .config("spark.hadoop.hive.merge.smallfiles.avgsize", "256000000")
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") //广播配置大小放开
      .enableHiveSupport()
      .getOrCreate
    sparkSession.sparkContext.setLogLevel("info")
    return sparkSession
  }

  def printAndExecuteSql(sql: String, spark: SparkSession): DataFrame = {
    val sqlLog = "==" * 10 + "sql start" + "==" * 10 + "\n" + sql + "\n" + "==" * 10 + "sql end" + "==" * 10 + "\n"
    println(sqlLog)
    spark.sql(sql)
  }

}
