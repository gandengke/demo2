package com.jd.easy.audience.task.plugin.step.dataset.udd

import java.io.File

import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.model.UddModel
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.{UddAssetsBean, UddBatchBean, UddChainFlowBean}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.generator.util.DateUtil
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.step.dataset.udd.engine.StatusGenerator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * 4a明细数据批量重跑历史
 */
class UddStatusGeneratorStepTest extends StepCommon[java.util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[UddStatusGeneratorStepTest])

  override def validate(): Unit = {
    val bean: UddBatchBean = getStepBean.asInstanceOf[UddBatchBean];

    if (null == bean.getEndPoint || null == bean.getBucket || null == bean.getAccessKey || null == bean.getSecretKey || null == bean.getOssFilePath) {
      throw new JobInterruptedException("UddStatusGeneratorStepTest oss配置参数错误", "Error: The Oss_Configuration of UddStatusGeneratorStepTest Error");
    } else if (null == bean.getUddModel || null == bean.getStatDate || null == bean.getEndDate) {
      throw new JobInterruptedException("UddStatusGeneratorStepTest 模型配置参数错误", "Error: The UddModel of UddStatusGeneratorStepTest Error");
    }
  }

  /**
   * Run a step from the dependency steps.
   *
   * @return T result set.
   * @throws Exception
   */
  override
  def run(dependencies: java.util.Map[String, StepCommonBean[_]]): java.util.Map[String, AnyRef] = {
    val bean: UddBatchBean = getStepBean.asInstanceOf[UddBatchBean];
    val statDate: String  = bean.getStatDate
    val endDate: String = bean.getEndDate
    val uddModel: UddModel = bean.getUddModel

    LOGGER.info("uddModel：" + bean.getUddModel.toString)
//    val node = "http://es-nlb-es-ekrtul421o.jvessel-open-jdstack.jdcloud.local"
//    val esport = "9200"

    implicit val sparkSession = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode, bean.getEsPort, bean.getEsUser, bean.getEsPassword,"UddStatusGeneratorStepTest");
    //1. 明细数据计算
    var dateList: java.util.List[String] = DateUtil.getDayList(statDate, endDate)
    import collection.JavaConversions._
    LOGGER.info("datelist:" + dateList.toString)
    for (dateEle <- dateList) {
      //status
      LOGGER.info("dateEle=" + dateEle.toString)
      val statusDF = new StatusGenerator(uddModel, bean.isUseOneId).execute(dateEle, false)
      statusToSave(statusDF, sparkSession, dateEle, bean)
      //assets
      var data: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]()

      var assetsStep: UddAssetsBean = new UddAssetsBean()
      assetsStep.setStatDate(dateEle)
      assetsStep.setDatasetId(String.valueOf(uddModel.getDatasetId))
      assetsStep.setEndPoint(bean.getEndPoint)
      assetsStep.setAccessKey(bean.getAccessKey)
      assetsStep.setSecretKey(bean.getSecretKey)
      assetsStep.setBucket(bean.getBucket)
      assetsStep.setOssFilePath(bean.getOssFilePath)
      assetsStep.setEsNode(bean.getEsNode)
      assetsStep.setEsIndex(bean.getEsassetsIndex)
      assetsStep.setEsPort(bean.getEsPort)
      assetsStep.setEsUser(bean.getEsUser)
      assetsStep.setEsPassword(bean.getEsPassword)


      val assetsstepObj = new UddAssetsGeneratorStep()
      assetsstepObj.setStepBean(assetsStep)
      assetsstepObj.validate()
      assetsStep.setData(assetsstepObj.run(null))

      var chainFlowStep: UddChainFlowBean = new UddChainFlowBean()
      chainFlowStep.setStatDate(dateEle)
      chainFlowStep.setDatasetId(String.valueOf(uddModel.getDatasetId))
      chainFlowStep.setEndPoint(bean.getEndPoint)
      chainFlowStep.setAccessKey(bean.getAccessKey)
      chainFlowStep.setSecretKey(bean.getSecretKey)
      chainFlowStep.setBucket(bean.getBucket)
      chainFlowStep.setOssFilePath(bean.getOssFilePath)
      chainFlowStep.setEsNode(bean.getEsNode)
      chainFlowStep.setEsIndex(bean.getEschainIndex)
      chainFlowStep.setEsPort(bean.getEsPort)
      chainFlowStep.setEsUser(bean.getEsUser)
      chainFlowStep.setEsPassword(bean.getEsPassword)
      chainFlowStep.setPeriod(bean.getPeriod)



      val chainstepObj = new UddChainFlowGeneratorStep()
      chainstepObj.setStepBean(chainFlowStep)
      chainstepObj.validate()
      chainFlowStep.setData(chainstepObj.run(null))
    }

    //2. 元数据信息返回
    val resultMap = new java.util.HashMap[String, Object]()
    LOGGER.info("resultMap=" + resultMap.toString)
    resultMap
  }

  /**
   * 明细数据保存到oss中
   *
   * @param df
   */
  private def statusToSave(df: DataFrame, sparkSession: SparkSession, date: String, bean: UddBatchBean): Unit = {

    var ossPathPrefix = bean.getBucket + File.separator + JdCloudOssUtil.dealPath(bean.getOssFilePath)
    var parquetFilePath = JdCloudOssUtil.getUddStatusDataKey(ossPathPrefix, date);
    LOGGER.info("statusToSave write to oss,bucket=" + bean.getBucket + ",parquetFilePath=" + parquetFilePath)
    val config: JdCloudDataConfig = new JdCloudDataConfig( bean.getEndPoint, bean.getAccessKey, bean.getSecretKey)
    config.setOptions(new java.util.HashMap[String, String]() {{put("header", "true")}})
    config.setClientType(ConfigProperties.getClientType)
    config.setFileType(OssFileTypeEnum.parquet)
    config.setData(df)
    config.setObjectKey(parquetFilePath)
    JdCloudOssUtil.writeDatasetToOss(sparkSession, config)
  }
}