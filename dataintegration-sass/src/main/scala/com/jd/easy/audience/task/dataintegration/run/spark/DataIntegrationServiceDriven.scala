package com.jd.easy.audience.task.dataintegration.run.spark

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.jd.easy.audience.common.error.SparkApplicationErrorResult
import com.jd.easy.audience.common.exception.{JobInterruptedException, JobNeedReTryException}
import com.jd.easy.audience.common.model.{AppContextJfsConf, SparkApplicationContext}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment
import com.jd.easy.audience.task.dataintegration.exception.IntegrationExceptionEnum
import com.jd.easy.audience.task.dataintegration.util.CustomSerializeUtils
import com.jd.easy.audience.task.driven.run.StepExecutor
import com.jd.jss.JingdongStorageService
import org.slf4j.LoggerFactory

/**
 * 数据集成服务任务统一入口
 */
object DataIntegrationServiceDriven {
  private val LOGGER = LoggerFactory.getLogger(DataIntegrationServiceDriven.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new JobInterruptedException("参数信息为空", "The parameters are empty!")
    }
    var applicationArgs: SparkApplicationContext = null
    var jfsConf: AppContextJfsConf = null
    var sparkSegment: SparkStepSegment = null
    var jfsClient: JingdongStorageService = null

    //1.解码入口任务参数
    val segmentJson: String = args(0)
    LOGGER.info("base64 arg: {}", segmentJson)
    val decodeJsonContext: String = new String(Base64.getDecoder.decode(segmentJson), StandardCharsets.UTF_8)
    LOGGER.info("DataIntegrationServiceDriven arg: {}", decodeJsonContext)
    applicationArgs = CustomSerializeUtils.deserialize(decodeJsonContext, classOf[SparkApplicationContext])

    //2.获取step参数
    //2.1 获取oss中step的段落数据
    jfsConf = applicationArgs.getAppContextJfsConf
    jfsClient = JdCloudOssUtil.createJfsClient(jfsConf.getEndPoint, jfsConf.getAppContextAccessKey, jfsConf.getAppContextSecretKey)
    try {
      val segment: String = JdCloudOssUtil.getObject(jfsClient, jfsConf.getAppContextBucket, applicationArgs.getSparkInputFile)
      LOGGER.info("segment:" + segment)
      //2.2 解析成对象并校验依赖
      sparkSegment = CustomSerializeUtils.createSegmentObject(segment).asInstanceOf[SparkStepSegment]
      LOGGER.info("after SegmentFactory.create segment= " + CustomSerializeUtils.serialize(sparkSegment))
    } catch {
      case e: Exception =>
        LOGGER.error("DataIntegrationServiceDriven deserialize error: ", e)
        throw new JobInterruptedException("pipeline 的segment定义错误", "Wrong pipeline segment define", e)
    }
    try {
      //3.任务执行
      if (JdCloudOssUtil.objectExist(jfsClient, jfsConf.getAppContextBucket, applicationArgs.getSparkErrorFile)) {
        jfsClient.deleteObject(jfsConf.getAppContextBucket, applicationArgs.getSparkErrorFile)
      }
      val outputMap = StepExecutor.run(sparkSegment.getName, sparkSegment.getBeans)
      LOGGER.info("spark application finished.")
      if (outputMap != null && outputMap.size > 0) {
        //4. 结果输出到oss
        LOGGER.info("map " + outputMap + " will be write to oss path " + applicationArgs.getSparkOutputFile)
        val objectStr: String = CustomSerializeUtils.encodeAfterObject2Json(outputMap)
        JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, jfsConf.getAppContextBucket, applicationArgs.getSparkOutputFile)
      }
    } catch {
      case e: Exception =>
        //5.异常统一处理
        jfsClient = JdCloudOssUtil.createJfsClient(jfsConf.getEndPoint, jfsConf.getAppContextAccessKey, jfsConf.getAppContextSecretKey)
        dealErrorInfo(jfsClient, e, applicationArgs)
    } finally {
      //6.清理现场
      jfsClient.destroy()
      StepExecutor.shutdownThreadPool()
    }
  }

  /**
   * 对异常信息进行统一处理
   *
   * @param jfsClient
   * @param e
   * @param applicationArgs
   */
  private def dealErrorInfo(jfsClient: JingdongStorageService, e: Exception, applicationArgs: SparkApplicationContext): Unit = {
    if (e.getCause == null) {
      //5.1 系统错误
      buildAppError(jfsClient, new JobNeedReTryException("系统错误", "Error: System Error!"), applicationArgs)
      throw e
    }
    val className: String = e.getCause.getClass.getSimpleName
    if (className == "JobInterruptedException") {
      //5.2 已经处理的异常-无需重试
      LOGGER.error("JobInterruptedException：", e)
      buildAppError(jfsClient, (e.getCause).asInstanceOf[JobInterruptedException], applicationArgs)
    }
    else if (className == "JobNeedReTryException") {
      //5.3 已经处理的异常-需重试
      LOGGER.error("JobNeedReTryException：", e)
      buildAppError(jfsClient, (e.getCause).asInstanceOf[JobNeedReTryException], applicationArgs)
    }
    else {
      //5.4 未处理异常，需要输出具体的异常内容
      val stackTraceElement: Array[StackTraceElement] = e.getCause.getStackTrace
      LOGGER.error("xmcauseby: ", e.getCause)
      LOGGER.error("异常名：" + stackTraceElement(0).toString)
      LOGGER.error("异常类名：" + stackTraceElement(0).getFileName)
      LOGGER.error("异常方法名：" + stackTraceElement(0).getMethodName)
      val stepName: String = stackTraceElement(0).getFileName.split("\\.")(0)
      val methodName: String = stackTraceElement(0).getMethodName
      val exceptionEnum: IntegrationExceptionEnum = IntegrationExceptionEnum.valueOf(stepName, methodName)
      var exception: Exception = null
      if (null != exceptionEnum && exceptionEnum.isNeedRetry) {
        exception = new JobNeedReTryException(exceptionEnum.getExceptionDesc, e)
      } else if (null != exceptionEnum && !exceptionEnum.isNeedRetry) {
        exception = new JobInterruptedException(exceptionEnum.getExceptionDesc, e)
      } else {
        exception = new JobNeedReTryException("系统错误", e.getCause)
      }
      buildAppError(jfsClient, exception, applicationArgs)
    }
    if (!e.isInstanceOf[JobInterruptedException]) {
      LOGGER.error("出现了需要重试的异常", e)
      throw new RuntimeException(e.getMessage, e)
    }

  }

  /**
   * 构建作业的错误信息并上传至jfs
   */
  private def buildAppError(jfsClient: JingdongStorageService, e: Exception, applicationArgs: SparkApplicationContext): Unit = {
    val errorResult = new SparkApplicationErrorResult
    errorResult.setErrorClass(e.getClass.toString)
    errorResult.setErrorMsg(e.getMessage)
    errorResult.setStackTraceJson(CustomSerializeUtils.serialize(e))
    if (e.isInstanceOf[JobNeedReTryException]) {
      val exception = e.asInstanceOf[JobNeedReTryException]
      errorResult.setErrorEnMsg(exception.getMessageEn)
    }
    else if (e.isInstanceOf[JobInterruptedException]) {
      val exception = e.asInstanceOf[JobInterruptedException]
      errorResult.setErrorEnMsg(exception.getMessageEn)
    } else {
      errorResult.setErrorEnMsg("Error: System Error!")
    }
    LOGGER.info("error orgin:" + errorResult.getErrorMsg)
    LOGGER.info("errorResult string=" + CustomSerializeUtils.serialize(errorResult))
    val objectStr = CustomSerializeUtils.encodeAfterObject2Json(errorResult)
    JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr,
      applicationArgs.getAppContextJfsConf.getAppContextBucket,
      applicationArgs.getSparkErrorFile)
  }


}
