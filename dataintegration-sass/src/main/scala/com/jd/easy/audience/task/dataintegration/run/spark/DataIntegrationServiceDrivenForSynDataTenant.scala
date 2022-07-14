package com.jd.easy.audience.task.dataintegration.run.spark

import java.io.File
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util
import java.util.Base64

import com.alibaba.fastjson
import com.jd.easy.audience.common.constant.{NumberConstant, StringConstant}
import com.jd.easy.audience.common.error.SparkApplicationErrorResult
import com.jd.easy.audience.common.exception.{JobInterruptedException, JobNeedReTryException}
import com.jd.easy.audience.common.model.{AppContextJfsConf, SparkApplicationContext}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.LoadData4TenantBean
import com.jd.easy.audience.task.commonbean.contant.BuffaloCircleTypeEnum
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment
import com.jd.easy.audience.task.dataintegration.exception.IntegrationExceptionEnum
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.{CustomSerializeUtils, SynDataFilterExpressParserEnum, TableMetaUtil}
import com.jd.easy.audience.task.driven.run.StepExecutor
import com.jd.easy.audience.task.driven.step.StepCommonBean
import com.jd.jss.JingdongStorageService
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

/**
 * 同步public库的数据到租户库中
 */
@Deprecated
object DataIntegrationServiceDrivenForSynDataTenant {
  private val LOGGER = LoggerFactory.getLogger(DataIntegrationServiceDrivenForSynDataTenant.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new JobInterruptedException("参数信息为空", "The parameters are empty!")
    }
    var applicationArgs: SparkApplicationContext = null
    var jfsConf: AppContextJfsConf = null
    var sparkSegment: SparkStepSegment = null
    var jfsClient: JingdongStorageService = null
    var accountId: String = null
    var accountName: String = null
    var dbName: String = null
    val buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME)
    val jobTime: DateTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val proAccountName: String = System.getenv(ConfigProperties.HADOOP_USER) //生产账号
    val paramsMap: util.Map[java.lang.String, java.lang.String] = new util.HashMap[java.lang.String, java.lang.String]() {
      put(SynDataFilterExpressParserEnum.DAY.getCode, jobTime.toString("yyyy-MM-dd"))
      put(SynDataFilterExpressParserEnum.BDP_PROD_ACCOUNT.getCode, proAccountName)
    }
    LOGGER.info("HADOOP_USER_NAME:" + proAccountName)

    //1.解码入口任务参数
    val segmentJson: String = args(0)
    val decodeJsonContext: String = new String(Base64.getDecoder.decode(segmentJson), StandardCharsets.UTF_8)
    LOGGER.info("DataIntegrationServiceDriven arg: {}", decodeJsonContext)
    applicationArgs = CustomSerializeUtils.deserialize(decodeJsonContext, classOf[SparkApplicationContext])

    //2.获取step参数
    //2.1 获取oss中step的段落数据
    jfsConf = applicationArgs.getAppContextJfsConf
    jfsClient = JdCloudOssUtil.createJfsClient(jfsConf.getEndPoint, jfsConf.getAppContextAccessKey, jfsConf.getAppContextSecretKey)
    try {
      val segment: String = JdCloudOssUtil.getObject(jfsClient, jfsConf.getAppContextBucket, applicationArgs.getSparkInputFile)
      //segment格式为ossUrl,dbName,accountId json格式
      LOGGER.info("segment:" + segment)
      //2.2 解析成对象并校验依赖
      val stepJson: fastjson.JSONObject = com.alibaba.fastjson.JSON.parseObject(segment)
      //2.3 从配置的oss中获取segment段落解析
      dbName = stepJson.getString("dbName")
      accountId = stepJson.getString("accountId")
      accountName = stepJson.getString("accountName")
      val ossUrl: String = stepJson.getString("ossUrl")
      //封装占位符需要的参数
      paramsMap.put(SynDataFilterExpressParserEnum.ACCOUNT_ID.getCode, accountId)
      if (StringUtils.isAnyBlank(ossUrl, dbName, accountName, accountId)) {
        LOGGER.error("DataIntegrationServiceDrivenForTenant argfile missing pipeline argument.: ")
        throw new JobInterruptedException("pipeline 的segment定义错误", "Wrong pipeline segment define")
      }
      val beanSegment: String = JdCloudOssUtil.getObject(jfsClient, jfsConf.getAppContextBucket, stepJson.getString("ossUrl"))
      LOGGER.info("beanSegment:" + beanSegment)
      //2.2 解析成对象并校验依赖
      sparkSegment = CustomSerializeUtils.createSegmentObject(beanSegment).asInstanceOf[SparkStepSegment]
    } catch {
      case e: Exception =>
        LOGGER.error("DataIntegrationServiceDrivenForTenant deserialize error: ", e)
        throw new JobInterruptedException("pipeline 的segment定义错误", "Wrong pipeline segment define", e)
    }
    try {
      //3.任务执行
      if (JdCloudOssUtil.objectExist(jfsClient, jfsConf.getAppContextBucket, applicationArgs.getSparkErrorFile)) {
        jfsClient.deleteObject(jfsConf.getAppContextBucket, applicationArgs.getSparkErrorFile)
      }
      //从配置中获取指定的参数文件内容
      val beansMap: util.Map[String, StepCommonBean[_]] = sparkSegment.getBeans
      import scala.collection.JavaConversions._
      val beansMapNew: util.Map[String, StepCommonBean[_]] = new util.HashMap[String, StepCommonBean[_]]()
      for ((k, v) <- beansMap) {
        val circlebean: LoadData4TenantBean = v.asInstanceOf[LoadData4TenantBean]
        circlebean.setAccountId(accountId.toLong)
        circlebean.setAccountName(accountName)
        circlebean.setDbName(dbName)
//        if (!circlebean.getSourceTableName.startsWith("adm_cdpsplitdata_module_")) {
//          //使用配置的过滤条件处理（天更新）
//          val curcirclebean: LoadData4TenantBean = BeanUtils.cloneBean(circlebean).asInstanceOf[LoadData4TenantBean]
//          val stepName = k + StringConstant.UNDERLINE + accountId + StringConstant.UNDERLINE + curcirclebean.getSourceTableName
//          curcirclebean.setStepName(stepName)
//          LOGGER.info("paramsMap:{},circlebean={},filterExpress={}", paramsMap.toString, JsonUtil.serialize(circlebean), curcirclebean.getFilterExpress)
//          val parseExpress = SynDataFilterExpressParserEnum.parse(circlebean.getFilterExpress, paramsMap)
//          //解析分区信息，是否存在需要更新的数据
//          val circleStep: Int = circlebean.getCircleStep
//          val circleType: BuffaloCircleTypeEnum = circlebean.getCircleType
//          val bizTime: DateTime = getBizTime(circleStep, circleType, jobTime)
//          val dataUri = ConfigProperties.DATABASE_URI + File.separator + ConfigProperties.PUBLIC_DB_NAME_DB + File.separator + circlebean.getSourceTableName + parseConditions(parseExpress)
//          val modificationinfos: util.Map[String, String] = TableMetaUtil.getDirectoryModificationTime(new Path(dataUri), fileSystem, bizTime, jobTime)
//          if (modificationinfos.size() > NumberConstant.INT_0) {
//
//          }
//          curcirclebean.setFilterExpress(parseExpress)
//          beansMapNew.put(stepName, curcirclebean)
//        } else {
          //动态获取需要更新的数据-模版数据更新
          val curcirclebean: LoadData4TenantBean = BeanUtils.cloneBean(circlebean).asInstanceOf[LoadData4TenantBean]
          curcirclebean.setStepName(k)
        dealLoadBeanData(curcirclebean, jobTime, proAccountName, beansMapNew)
//        }
      }
      LOGGER.info("beansMapNew=" + CustomSerializeUtils.serialize(beansMapNew))
      val outputMap = StepExecutor.run(sparkSegment.getName, beansMapNew)
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
    JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, applicationArgs.getAppContextJfsConf.getAppContextBucket, applicationArgs.getSparkErrorFile)
  }

  private def getBizTime(circleStep: Int, circleType: BuffaloCircleTypeEnum, jobTime: DateTime): DateTime = {
    var bizTime = jobTime

    if (StringConstant.BUFFALO_CIRLE_HOUR.equalsIgnoreCase(circleType.getValue)) { // 按照每小时加工
      bizTime = jobTime.minusHours(circleStep)
    }
    else if (StringConstant.BUFFALO_CIRLE_MINUTE.equalsIgnoreCase(circleType.getValue)) { // 按照分钟加工
      bizTime = jobTime.minusMinutes(circleStep)
    }
    else if (StringConstant.BUFFALO_CIRLE_DAY.equalsIgnoreCase(circleType.getValue)) bizTime = jobTime.minusDays(circleStep)
    else {
      LOGGER.error("不支持时，分周期之外的任务类型")
      throw new JobInterruptedException("任务周期类型异常")
    }
    bizTime
  }

  /**
   * 根据不同场景对数据进行拆分
   * @param circlebean
   * @param jobTime
   * @param prodAccount
   * @param beansMapNew
   */
  private def dealLoadBeanData(circlebean: LoadData4TenantBean, jobTime: DateTime, prodAccount: String, beansMapNew: util.Map[String, StepCommonBean[_]]): Unit = {
    val fileSystem = FileSystem.get(TableMetaUtil.getHadoopConf(prodAccount))
    val tableName: String = circlebean.getSourceTableName
    val circleStep: Int = circlebean.getCircleStep
    val circleType: BuffaloCircleTypeEnum = circlebean.getCircleType
    val bizTime: DateTime = getBizTime(circleStep, circleType, jobTime)
    val sd = new SimpleDateFormat(StringConstant.USUAL_DATE_FORMAT)
    val dataUri = ConfigProperties.DATABASE_URI + File.separator + ConfigProperties.PUBLIC_DB_NAME_DB + File.separator + tableName
    val prodUri = dataUri + File.separator + "bdp_prod_account=" + prodAccount
    LOGGER.info("ready to excute! statDate=" + sd.format(bizTime.toDate) + ",tablename=" + tableName + ",prodAccountName=" + prodAccount + ",biztime=" + sd.format(bizTime.toDate) + ",jobTime=" + sd.format(jobTime.toDate))
    val modificationinfos: util.Map[String, String] = TableMetaUtil.getDirectoryModificationTime(new Path(prodUri), fileSystem, bizTime, jobTime)
    //过滤目录
    LOGGER.info(tableName + " modifiedTime:" + modificationinfos.toString)
    val uriKeys: util.Set[String] = modificationinfos.keySet
    import scala.collection.JavaConversions._
    val condExpress = dealUri(uriKeys, tableName)
    for (bean <- condExpress) {
      //hdfs路径，解析分区数据
      if (!beansMapNew.containsKey(bean._1)) {
            val curcirclebean: LoadData4TenantBean = BeanUtils.cloneBean(circlebean).asInstanceOf[LoadData4TenantBean]
            curcirclebean.setStepName(bean._1)
            curcirclebean.setFilterExpress(bean._2)
            beansMapNew.put(bean._1, curcirclebean)
      }
//      val condExpress = dealUri(uri.substring(dataUri.length + NumberConstant.INT_1))
//      LOGGER.info("condMap:{}", condExpress)
//      val sourceInfo: String = circlebean.getStepName + StringConstant.UNDERLINE + circlebean.getAccountId + StringConstant.UNDERLINE + tableName + StringConstant.UNDERLINE
//      if (!beansMapNew.containsKey(sourceInfo)) {
//        val curcirclebean: LoadData4TenantBean = BeanUtils.cloneBean(circlebean).asInstanceOf[LoadData4TenantBean]
//        curcirclebean.setStepName(sourceInfo)
//        curcirclebean.setFilterExpress(condExpress)
//        beansMapNew.put(sourceInfo, curcirclebean)
//      }
    }
  }
  /**
   * 解析字段和条件，便于拼接hdfs目录
   * @param filterExpress
   * @return
   */
  private def  parseConditions(filterExpress: String) : String ={
    var hdfsExtend = ""
    if (StringUtils.isBlank(filterExpress)) {
      return hdfsExtend
    }
    val expressArr: Array[String] = filterExpress.split("AND")
    expressArr.foreach(expressItem => {
      val valueArr: Array[String] = expressItem.split("=")
      var fieldVal = StringUtils.trim(valueArr(1))
      if (fieldVal.startsWith("\"")) {
        fieldVal = fieldVal.substring(1)
      }
      if (fieldVal.endsWith("\"")) {
        fieldVal = fieldVal.substring(0, fieldVal.length -1)
      }
      hdfsExtend += File.separator + StringUtils.trim(valueArr(0)) + "=" + fieldVal
    })
    return hdfsExtend
  }

  /**
   * 通过hdfs路径解析分区信息，组装过滤条件
   * @param partitionsHdfs
   * @return
   */
  private def  parsePartitionCond(partitionsHdfs: String) : util.LinkedHashMap[String, String] ={
    val condMap: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
    if (StringUtils.isBlank(partitionsHdfs)) {
      return condMap
    }
    val expressArr: Array[String] = partitionsHdfs.split(File.separator)
    var dtExpress: String = ""
    var sourceExpress: String = ""
    expressArr.foreach(expressItem => {
      val valueArr: Array[String] = expressItem.split("=")
      var fieldVal = StringUtils.trim(valueArr(1))
      condMap.put(StringUtils.trim(valueArr(0)), fieldVal)
    })
    if (condMap.containsKey("source_id")) {
      //有sourceId的表需要忽略dt字段
      condMap.remove("dt")
    }
    return condMap
  }

  /**
   * 通过uri处理需要同步的过滤条件
   * @param uriModifiedInfo
   * @return
   */
  private def dealUri(uriModifiedInfo: util.Set[String], tableName: String): util.Map[String, String] = {
    val dataUri = ConfigProperties.DATABASE_URI + File.separator + ConfigProperties.PUBLIC_DB_NAME_DB + File.separator + tableName
    val beancondMap: util.Map[String, String] = new util.HashMap[String, String]()
    import scala.collection.JavaConversions._
    for (uri <- uriModifiedInfo) {
      val condMap = parsePartitionCond(uri.substring(dataUri.length + NumberConstant.INT_1))
      val stepName = tableName + StringConstant.UNDERLINE + StringUtils.join(condMap.values(), StringConstant.UNDERLINE)
      if (!beancondMap.containsKey(stepName)) {
        val filter: util.List[String] = new util.ArrayList[String]()
        for (cond <- condMap) {
          filter.add(cond._1 + "=\"" + cond._2 + "\" ")
        }
        beancondMap.put(stepName, StringUtils.join(filter, " and "))
      }
    }
    return beancondMap
  }
}
