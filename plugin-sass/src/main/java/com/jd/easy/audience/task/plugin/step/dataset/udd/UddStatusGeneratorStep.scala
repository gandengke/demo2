package com.jd.easy.audience.task.plugin.step.dataset.udd

import java.io.File

import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssClientTypeEnum, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.UddStatusBean
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.step.dataset.udd.engine.StatusGenerator
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer

/**
 * 4a明细数据
 */
class UddStatusGeneratorStep extends StepCommon[Unit] {
  private val LOGGER = LoggerFactory.getLogger(classOf[UddStatusGeneratorStep])

  override def validate(): Unit = {
    val bean: UddStatusBean = getStepBean.asInstanceOf[UddStatusBean];
    if (null == bean.getEndPoint || null == bean.getBucket || null == bean.getAccessKey || null == bean.getSecretKey || null == bean.getOssFilePath) {
      throw new JobInterruptedException("uddStatusGeneratorStep oss配置参数错误", "Error: The Oss_Configuration of uddStatusGeneratorStep Error");
    } else if (null == bean.getUddModel || null == bean.getStatDate) {
      throw new JobInterruptedException("uddStatusGeneratorStep 模型配置参数错误", "Error: The UddModel of uddStatusGeneratorStep Error");
    }
  }

  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: java.util.Map[String, StepCommonBean[_]]): Unit = {
    //1. 初始化
    val bean: UddStatusBean = getStepBean.asInstanceOf[UddStatusBean];
    LOGGER.info("uddModel：" + bean.getUddModel.toString)
    implicit val sparkSession = SparkLauncherUtil.buildSparkSession(if (StringUtils.isBlank(bean.getStepName)) "UddStatusStep" else bean.getStepName);

    val dbName = bean.getUddModel.getRelDataset.getData.get(0).getDatabaseName
    sparkSession.sql("use " + dbName)
    //2. 校验表是否存在
    val tableList = bean.getUddModel.getRelDataset.getData
    for (i <- NumberConstant.INT_0 until tableList.size) {
      val tb = tableList(i)
      if (!sparkSession.catalog.tableExists(tb.getDatabaseName, tb.getDataTable)) {
        throw new JobInterruptedException(s"""${tb.getDataTable} 表不存在，请重新检查""", "Failure: The Table " + tb.getDataTable + " is not exist!")
      }
    }

    //3. 清理oss数据
    val jfsClient = JdCloudOssUtil.createJfsClient(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
    val dataPath = JdCloudOssUtil.dealPath(bean.getOssFilePath) + File.separator + bean.getStatDate.replaceAll(StringConstant.MIDDLELINE, StringConstant.EMPTY)
    JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket, dataPath, null)
    jfsClient.destroy()
    //4. 明细数据计算
    val statusDF = calStatusData(sparkSession, bean, false)
    //5. 明细数据保存
    statusToSave(statusDF, sparkSession, bean)
  }

  /**
   * 计算明细数据
   */
  def calStatusData(implicit spark: SparkSession, bean: UddStatusBean, isTest: Boolean): DataFrame = {
    //1. 明细数据计算
    var isOneIdRet: Boolean = bean.isUseOneId
    val statusDF = new StatusGenerator(bean.getUddModel, isOneIdRet).execute(bean.getStatDate, isTest).filter("user_id is not null and user_id !=''")
    //TODO 验证数据
    statusDF.show(NumberConstant.INT_10)
    statusDF
  }

  /**
   * 明细数据保存到oss中
   *
   * @param df
   */
  private def statusToSave(df: DataFrame, sparkSession: SparkSession, bean: UddStatusBean): Unit = {
    var ossPathPrefix = bean.getBucket + File.separator + JdCloudOssUtil.dealPath(bean.getOssFilePath)
    var parquetFilePath = JdCloudOssUtil.getUddStatusDataKey(ossPathPrefix, bean.getStatDate);
    LOGGER.info("statusToSave write to oss,bucket=" + bean.getBucket + ",parquetFilePath=" + parquetFilePath)
    var dataConfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
    dataConfig.setClientType(ConfigProperties.getClientType)
    dataConfig.setFileType(OssFileTypeEnum.parquet)
    dataConfig.setObjectKey(parquetFilePath)
    dataConfig.setData(df)
    dataConfig.setOptions(new java.util.HashMap[String, String]() {
      {
        put("header", "true")
      }
    })
    JdCloudOssUtil.writeDatasetToOss(sparkSession, dataConfig)
  }
}