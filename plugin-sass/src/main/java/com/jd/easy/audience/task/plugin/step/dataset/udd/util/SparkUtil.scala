package com.jd.easy.audience.task.plugin.step.dataset.udd.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * spark工具类
  */
object SparkUtil {


  /**
    * 获取spark连接ES配置
    *
    * @return
    */
  def getSparkEsConfig( esNodes: String, esPort: String, esUser: String, esPass: String): SparkConf = {
    val conf = new SparkConf()
    conf.set("es.port", esPort)
    conf.set("es.nodes", esNodes)
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user", esUser) //访问es的用户名
    conf.set("es.net.http.auth.pass", esPass)
    conf.set("es.nodes.discovery", "true")
    conf
  }

  /**
    * DataFrame 写入hive表
    *
    * @param spark     SparkSession
    * @param dfToWrite DataFrame
    * @param tableName tableName
    * @param saveMode  SaveMode
    */
  def dfSaveToHiveTb(spark: SparkSession,
                     dfToWrite: DataFrame,
                     tableName: String,
                     saveMode: SaveMode = SaveMode.Overwrite
                    ): Unit = {
    dfToWrite.write.format("orc").mode(saveMode).insertInto(tableName)
  }

  /**
    * 合并df
    * @param spark  SparkSession
    * @param parentDf DataFrame
    * @param df DataFrame
    * @return
    */
  def dfUnion(spark: SparkSession, parentDf: DataFrame, df: DataFrame): DataFrame = {
    if (parentDf.equals(spark.emptyDataFrame)) {
      df
    } else {
      parentDf.unionAll(df)
    }
  }

}
