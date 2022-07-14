package com.jd.easy.audience.task.plugin.step.audience

import com.jd.easy.audience.common.constant.{LabelSourceEnum, NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.{AudiencePortraitProfileBean, CommonLabelSetInfolBean}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.util.DbManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.math.BigInteger
import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, HashMap}
import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.util.control.Breaks.{break, breakable}

/**
 * 360画像任务加工
 *
 * @author cdxiongmei
 * @date 2022/2/15 下午3:01
 * @version V1.0
 */
class AudiencePortraitDetailStep extends StepCommon[util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[AudiencePortraitDetailStep])
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val UPDATE_LIMITNUM = NumberConstant.INT_40

  private val AUDIENCE_PORTRAIT_DETAIL_FILENAME = "portrait_det.orc"
  private val AUDIENCE_PORTRAIT_LABEL_TB = "ea_audience_portrait_label"
  private val LABEL_VALUE_MAPPING_TB =  "ea_label_value_mapping"
  private val LABEL_MAPPING_TB =  "ea_label_mapping"

  private val LABELVAL_MAPPING_FIELD = "name,description,label_name,create_time,update_time,is_deleted"
  private val LABLE_MAPPING_FIELD = "name,description,data_from,tab,tab_value, type,is_default,is_rotate,update_time,is_deleted"

  override def run(dependencies: util.Map[String, StepCommonBean[_]]) = {

    // 1.环境准备
    val sparkSession: SparkSession = SparkLauncherUtil.buildSparkSession("AudiencePortraitDetailStep")
    val bean: AudiencePortraitProfileBean = getStepBean.asInstanceOf[AudiencePortraitProfileBean]
    val updateTime = new Date
    val curDate = SIMPLE_DATE_FORMAT.format(updateTime)

    // 2.标签信息处理
    val (labelsAll, tagsAll) = labelInit(sparkSession, bean.getLabelSets, curDate)

    // 3.人群画像明细
    val portraitDetail = getLabelData(sparkSession, labelsAll, bean)

    // 4.保存人群画像明细
    val outputMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    val idTypesIter = portraitDetail.keySet().iterator()
    if (bean.getType.toInt == 1) {
      // 4.1 直接求交人群
      while (idTypesIter.hasNext){
        val idType = idTypesIter.next()
        var tagsMap = new util.HashMap[String, util.List[TagsBean]]()
        // 5.画像明细保存到oss
        val config: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
        config.setOptions(new util.HashMap[String, String]() {{put("header", "true")}})
        config.setClientType(ConfigProperties.getClientType)
        config.setFileType(OssFileTypeEnum.orc)
        config.setData(portraitDetail.get(idType))
        config.setObjectKey(bean.getBucket + File.separator + bean.getOutputPath + File.separator + AUDIENCE_PORTRAIT_DETAIL_FILENAME)
        JdCloudOssUtil.writeDatasetToOss(sparkSession, config)

        // 6.写入标签和标签枚举信息
        tagsMap.put("tags", tagsAll.get(idType))
        writeAudiencePortraitLabels(labelsAll.get(idType), tagsMap.toString, bean, curDate)
        // 7.返回值封装
        outputMap.put("detailPath", bean.getOutputPath + File.separator + AUDIENCE_PORTRAIT_DETAIL_FILENAME)
        outputMap.put("tags", tagsAll.get(idType).toString)
      }
    } else {
      // 4.2 组合求交人群
      var output: util.List[util.Map[String, AnyRef]] = new util.ArrayList[util.Map[String, AnyRef]]
      var outputTags: util.List[util.Map[String, AnyRef]] = new util.ArrayList[util.Map[String, AnyRef]]
      var tableNames = new util.ArrayList[String]()
      var labels = new util.ArrayList[String]()
      while (idTypesIter.hasNext) {
        val outputMapCombine: util.Map[String, AnyRef] = new util.LinkedHashMap[String, AnyRef]
        val outputMapCombineTags: util.Map[String, AnyRef] = new util.LinkedHashMap[String, AnyRef]
        val idType = idTypesIter.next()
//        val ckTableName= idType + "_" + bean.getPortraitId
        val ckTableName= idType + "_" + "888888"
        labels.addAll(labelsAll.get(idType))
        tableNames.add(ckTableName)
        LOGGER.info("final combine: " + idType)

        // 5.1 建ck表

        // 5.2 画像明细结果保存到ck
        val config: Map[String, String] = Map[String, String](
          "batchsize" -> "50000",
          "isolationLevel" -> "NONE",
          "numPartitions" -> "1"
        )
        // 目标ClickHouse集群地址
        val url= "jdbc:clickhouse://service-ck-put3ayb763.ck-put3ayb763-hb.jvessel2.jdcloud.com:8123"
        // 目标ClickHouse集群中创建的数据库账号名, 密码, driver
        val prop = new java.util.Properties
        prop.setProperty("user", "qt_cu_rw")
        prop.setProperty("password", "$RWcdp_pwd_2022_ck")
        prop.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//        prop.setProperty("driver", "com.github.housepower.jdbc.ClickHouseDriver")

        val portraitDetailData = portraitDetail.get(idType).toDF
        portraitDetailData.write
          .mode(SaveMode.Append)
          .options(config)
          .jdbc(url, ckTableName, prop)
        LOGGER.info("data successfully written!")

        // 6.返回值封装
        outputMapCombine.put("path", ckTableName)
        outputMapCombine.put("pathType", "ck")
        outputMapCombine.put("tags", tagsAll.get(idType).toString)
        output.add(outputMapCombine)

        outputMapCombineTags.put("\"path\"", "\"" + ckTableName + "\"")
        outputMapCombineTags.put("\"pathType\"", "\"" + "ck" + "\"")
        outputMapCombineTags.put("\"tags\"", tagsAll.get(idType).toString)
        outputTags.add(outputMapCombineTags)
      }
      // 7.写入标签和标签枚举信息
      writeAudiencePortraitLabels(labels, tableNames, outputTags.toString.replaceAll("=",":").replace("\"", "\\\""), bean, curDate)
      outputMap.put("data", output)
    }

    LOGGER.info("outputMap：" + outputMap.toString)
    bean.setOutputMap(outputMap)
    outputMap
  }

/**
 * @description 保存画像枚举枚举信息
 * @param [valuesList, labelName, updateTime]
 * @return void
 * @date 2022/3/9 上午10:27
 * @auther cdxiongmei
 */
  def writePortraitValues(valuesList: util.Map[java.lang.Long, String], labelName: String, updateTime: String): Unit = {
    val valueList = new util.ArrayList[String]
    valuesList.foreach(
      valueItem => {
        valueList.add("(" + valueItem._1 + ",\"" + valueItem._2 + "\",\"" + labelName + "\",\"" + updateTime + "\",\"" + updateTime + "\",0)")
        if (valueList.size >= UPDATE_LIMITNUM) {
          insertMappingSql(valueList, LABELVAL_MAPPING_FIELD, LABEL_VALUE_MAPPING_TB, true)
          valueList.clear()
        }
      }
    )
    insertMappingSql(valueList, LABELVAL_MAPPING_FIELD, LABEL_VALUE_MAPPING_TB, true)
  }

/**
 * @description 保存人群画像标签数据
 * @param [labels, bean, updateTime]
 * @return void
 * @date 2022/3/9 上午10:27
 * @auther cdxiongmei
 */
  def writeAudiencePortraitLabels(labels: util.List[String], tags: String, bean: AudiencePortraitProfileBean, updateTime: String): Unit = {
    val insertSql = "UPDATE " + AUDIENCE_PORTRAIT_LABEL_TB +
      " SET portrait_url=\"" + bean.getOutputPath + File.separator + AUDIENCE_PORTRAIT_DETAIL_FILENAME +
      "\", tags=\"" + tags +
      "\", labels=\"" + StringUtils.join(labels, StringConstant.COMMA) +
      "\", update_time=\"" + updateTime +
      "\" WHERE is_deleted = 0 AND audience_id = " + bean.getAudienceId + " AND portrait_id = " + bean.getPortraitId

    // 写入数据
    updateSql(insertSql)
  }

  def writeAudiencePortraitLabels(labels: util.List[String], tablesName: util.List[String], tags: String, bean: AudiencePortraitProfileBean, updateTime: String): Unit = {
    val insertSql = "UPDATE " + AUDIENCE_PORTRAIT_LABEL_TB +
      " SET portrait_url=\"" + StringUtils.join(tablesName, StringConstant.COMMA) +
      "\", tags=\"" + tags +
      "\", labels=\"" + StringUtils.join(labels, StringConstant.COMMA) +
      "\", update_time=\"" + updateTime +
      "\" WHERE is_deleted = 0 AND audience_id = " + bean.getAudienceId + " AND portrait_id = " + bean.getPortraitId
    LOGGER.info("insertSQL: " + insertSql)

    // 写入数据
    updateSql(insertSql)
  }

  private def insertMappingSql(insertValueList: util.List[String], fields: String, tableName: String, isReplace: Boolean) = {
    var insertMark = "INSERT INTO ";
    if (isReplace) {
      insertMark = "REPLACE INTO ";
    }
    val insertSql = insertMark + tableName + "( " + fields + ")\nVALUES " + StringUtils.join(insertValueList, ",")
    updateSql(insertSql)
  }

  private def updateSql(sql: String) = {
    val conn = DbManager.getConn
    val stmt = DbManager.stmt(conn)
    val deleteRow = DbManager.executeUpdate(stmt, sql)
    if (deleteRow > BigInteger.ZERO.intValue) {
      LOGGER.info("数据更新成功rows = {}", deleteRow)
    }
    DbManager.close(conn, stmt, null, null)
  }

  /**
   * 从oss读取人群包明细数据
   *
   * @param spark
   * @return
   */
  def getAudienceData(spark: SparkSession, bean: AudiencePortraitProfileBean): Dataset[Row] = {

    // 1.jsf协议的oss路径
    val jfsPath = bean.getBucket + File.separator + URI.create(bean.getAudienceDataFile).getPath

    // 2.读取oss数据
    val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
    readconfig.setOptions(new HashMap[String, String]() {{put("header", "true")}})
    readconfig.setClientType(ConfigProperties.getClientType)
//    readconfig.setFileType(OssFileTypeEnum.orc)
    readconfig.setFileType(OssFileTypeEnum.csv)
    readconfig.setObjectKey(jfsPath)

    var dataset: Dataset[Row] = null
    if (bean.getType.toInt == 1) {
      dataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig).toDF("user_id")
    } else {
      dataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig).toDF("user_id", "id_type")
    }

    // 3.校验人群包数据
    if (null == dataset || dataset.count == BigInteger.ZERO.intValue) {
      throw new JobInterruptedException("没有查到该人群生成结果数据, 人群路径 = " + jfsPath, "No such audience package, jfsPath = " + jfsPath)
    }
    LOGGER.info("从oss获取人群包信息jfsPath = {}, audience size = {}。", jfsPath, dataset.count)
    dataset
  }

  /**
   * 获取标签值编码信息
   *
   * @param sparkSession
   * @param labels
   * @return
   */
  def labelInit(sparkSession: SparkSession, labels: util.List[CommonLabelSetInfolBean], updateTime: String) = {

    // label_mapping数据写入sql组装
    val labelInsertSqlList = new util.ArrayList[String]
    // 注册udf函数时使用，用于标签枚举的映射
    var labelInfo = new util.HashMap[String, util.HashMap[String, util.Map[String, String]]]
    // 存储所有标签字段信息
    var labelsAll: util.HashMap[String, util.List[String]] = new util.HashMap[String, util.List[String]]()
    // 封装tags输出到output中
    var tagsAll: util.HashMap[String, util.List[TagsBean]] = new util.HashMap[String, util.List[TagsBean]]()

    for (setItem <- labels) {
      val idType = setItem.getIdType
      if (LabelSourceEnum.custom.equals(setItem.getLabelSource) || LabelSourceEnum.common.equals(setItem.getLabelSource)) {
        setItem.getLabelValueDetails.foreach(label => {
          val labelName: String = LabelSourceEnum.getLabelPrefix(setItem.getLabelSource) + label.getLabelId;
          labelInsertSqlList.add("(\"" + labelName + "\",\"" +
            label.getLabelName + "\",3,\"" +
            label.getTab + "\",\"" +
            label.getTabValue + "\",\"" +
            label.getType + "\",\"" +
            label.getIsDefault + "\",\"" +
            label.getIsRotate + "\",\"" +
            updateTime + "\",0)"
          )
          if (labelInsertSqlList.size() >= UPDATE_LIMITNUM) {
            insertMappingSql(labelInsertSqlList, LABLE_MAPPING_FIELD, LABEL_MAPPING_TB, true)
            labelInsertSqlList.clear()
          }
          var labelsAllId: util.List[String] = labelsAll.getOrDefault(idType, new util.ArrayList[String]())
          labelsAllId.add(labelName)
          labelsAll.put(idType, labelsAllId)

          // value_mapping数据写入
          writePortraitValues(label.getValueMapInfo, labelName, updateTime)
          val valueMapping = new util.HashMap[String, String]
          var values: util.List[String] = new util.ArrayList[String]()
          label.getValueMapInfo.foreach {
            valueInfo => {
              valueMapping.put(valueInfo._2, valueInfo._1.toString)
              values.add(valueInfo._1.toString)
            }
          }
          var tagsAllId: util.List[TagsBean] = tagsAll.getOrDefault(idType, new util.ArrayList[TagsBean])
          var tagType = "single"
          if (values.size() > 1)
            tagType = "multi"
          tagsAllId.add(new TagsBean(labelName, tagType, values))
          tagsAll.put(idType, tagsAllId)

          if (LabelSourceEnum.custom.equals(setItem.getLabelSource)) {
            var labelInfoId = labelInfo.getOrDefault(idType, new util.HashMap[String, util.Map[String, String]]())
            labelInfoId.put(labelName, valueMapping)
            labelInfo.put(idType, labelInfoId)
          } else {
            var labelInfoId = labelInfo.getOrDefault(idType, new util.HashMap[String, util.Map[String, String]]())
            labelInfoId.put(LabelSourceEnum.getLabelPrefix(setItem.getLabelSource) + label.getLabelName, valueMapping)
            labelInfo.put(idType, labelInfoId)
          }
        })
      }
    }
    // label_mapping数据写入
    if (labelInsertSqlList.size() > NumberConstant.INT_0) {
      insertMappingSql(labelInsertSqlList, LABLE_MAPPING_FIELD, LABEL_MAPPING_TB, true)
    }
    LOGGER.info("labelsAll：" + labelsAll.toString)

    // 注册udf函数，进行标签枚举值的映射
    sparkSession.udf.register("getLabelValueCode", (labelName: String, value: String, idType: String) => {
      if (labelInfo.containsKey(idType)){
        if (labelInfo.get(idType).containsKey(labelName)) {
          if (labelInfo.get(idType).get(labelName).containsKey(value)) {
            labelInfo.get(idType).get(labelName).get(value)
          } else {
            val valList = value.split(File.separator)
            labelInfo.get(idType).get(labelName).get(valList(NumberConstant.INT_0))
          }
        } else {
          NumberConstant.N_LONG_1.toString
        }
      } else {
        NumberConstant.N_LONG_1.toString
      }
    })
    (labelsAll, tagsAll)
  }

  /**
   * 计算统一标签模型的画像明细数据
   * @param sparkSession
   * @param audienceDataset
   * @param setItem
   * @param labelAll
   * @return
   */
 def dealCommonLabelData(sparkSession: SparkSession, audienceDataset: Dataset[Row], setItem: CommonLabelSetInfolBean, labelAll: util.HashMap[String, util.List[String]]): String = {

   // 统一标签模型展开
   val labelSelect = new util.ArrayList[String]
   setItem.getLabelValueDetails.foreach(
     label => {
      labelSelect.add(LabelSourceEnum.getLabelPrefix(LabelSourceEnum.common) + label.getLabelId)
   })

   // 获取统一标签数据集数据并对人群数据集打标
   var commonLabelData = sparkSession.sql("SELECT user_id, label_name, label_value FROM " + setItem.getSourceUrl + " WHERE set_id = '" + setItem.getSetId + "'")
   commonLabelData = commonLabelData.join(audienceDataset, "user_id").cache()

   var unionList: util.List[String] = new util.ArrayList[String]
   for (i <- NumberConstant.INT_0 until setItem.getLabelValueDetails.size) {
     val labelEle = setItem.getLabelValueDetails.get(i)
     commonLabelData.filter("label_name = \"" + labelEle.getLabelName + "\"").createOrReplaceTempView("commontempview_" + labelEle.getLabelId)
     var tempSelectExpr: util.List[String] = new util.ArrayList[String]
     labelSelect.foreach(ele => tempSelectExpr.add("\"-1\" AS " + ele))
     tempSelectExpr.set(i, "getLabelValueCode(\"common_" + labelEle.getLabelName + "\", `label_value`, \"" + setItem.getIdType + "\") as common_" + labelEle.getLabelId)
     val sqlUnion = "SELECT user_id," + StringUtils.join(tempSelectExpr, StringConstant.COMMA) + " FROM commontempview_" + labelEle.getLabelId
     unionList.add(sqlUnion)
   }

   val fields = new util.ArrayList[String]
   fields.addAll(labelAll.get(setItem.getIdType))

   val maxExpress = fields.map(label => {
     if (labelSelect.contains(label)) {
       "MAX(`" + label + "`) AS " + label
     } else {
       "\"-1\" AS " + label
     }
   }).toList.asJava

   "SELECT user_id," + StringUtils.join(maxExpress, StringConstant.COMMA) + " FROM (" + StringUtils.join(unionList, " UNION ALL ") + ") GROUP BY user_id"
 }

  /**
   * 计算自定义标签数据的画像明细
   *
   * @param audienceLabelData
   * @param setItem
   * @param labelAll
   * @return
   */
  def dealCustomLabelData(audienceLabelData: Dataset[Row], setItem: CommonLabelSetInfolBean, labelAll: util.HashMap[String, util.List[String]]): String = {

    LOGGER.info(s"自定义标签模型datasetId = ${setItem.getDatasetId}, sourceUrl = ${setItem.getSourceUrl}")
    audienceLabelData.cache

    if (audienceLabelData.count <= NumberConstant.INT_0) {
      LOGGER.info("join with audience result count = 0")
      StringConstant.EMPTY
    } else {
      audienceLabelData.show(BigInteger.TEN.intValue)
      // 解析标签的tag信息（将多值的字段进行展开）
      val labelIds = setItem.getLabelValueDetails.map(label => LabelSourceEnum.getLabelPrefix(LabelSourceEnum.custom) + label.getLabelId)
      audienceLabelData.createOrReplaceTempView("customview_" + setItem.getDatasetId)
      val fieldInfos = new util.ArrayList[String]()
      fieldInfos.addAll(labelAll.get(setItem.getIdType))
      for (i <- NumberConstant.INT_0 until fieldInfos.size) {
        if (labelIds.contains(fieldInfos.get(i))) {
          fieldInfos.set(i, "getLabelValueCode(\"" + fieldInfos.get(i) + "\",`" + fieldInfos.get(i) + "`, \"" + setItem.getIdType + "\") AS " + fieldInfos.get(i))
        } else {
          fieldInfos.set(i, "\"-1\" AS " + fieldInfos.get(i))
        }
      }
      "SELECT user_id," + StringUtils.join(fieldInfos, StringConstant.COMMA) + " FROM customview_" + setItem.getDatasetId
    }
  }

  /**
   * 获取标签数据集标签明细数据
   *
   * @param spark
   * @return
   */
  def getLabelData(spark: SparkSession, labelAll: util.HashMap[String, util.List[String]], bean: AudiencePortraitProfileBean): util.HashMap[String, Dataset[Row]] = {

    import scala.collection.JavaConversions._
    var res: util.HashMap[String, Dataset[Row]] = new util.HashMap[String, Dataset[Row]]()

    // 加载人群数据
    val audienceDataset: Dataset[Row] = getAudienceData(spark, bean)

    // 直接求交人群
    if (bean.getType.toInt == 1) {
      audienceDataset.filter("user_id IS NOT NULL").cache
      var portraitList: util.List[String] = new util.ArrayList[String]()
      var idType: String = null
      var labels: util.List[String] = new util.ArrayList[String]()
      for (setItem <- bean.getLabelSets) {
        idType = setItem.getIdType
        labels.addAll(labelAll.get(idType))
        if (LabelSourceEnum.custom == setItem.getLabelSource) {
          // 1.获取自定义标签数据集
          val jfsObjectKey = bean.getBucket + File.separator + setItem.getSourceUrl
          val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
          readconfig.setOptions(new util.HashMap[String, String]() {{put("header", "true")}})
          readconfig.setClientType(ConfigProperties.getClientType)
          readconfig.setFileType(OssFileTypeEnum.csv)
          readconfig.setObjectKey(jfsObjectKey)
          val labelDataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig)
          // 2.对人群数据集打标
          var audienceLabelData = labelDataset.join(audienceDataset, "user_id")
          if (null == audienceLabelData || audienceLabelData.count == BigInteger.ZERO.intValue) {
            throw new JobInterruptedException("没有可用的数据", "No audience profile data")
          }
          val portraitSql = dealCustomLabelData(audienceLabelData, setItem, labelAll)
          if (StringUtils.isNotBlank(portraitSql)) {
            portraitList.add(portraitSql)
          }
        } else if (LabelSourceEnum.common.equals(setItem.getLabelSource)) {
          LOGGER.info("统一标签模型datasetId = {}", setItem.getDatasetId)
          // 多值字段和枚举字段需要拆分label_value字段
          val commonPortrait = dealCommonLabelData(spark, audienceDataset, setItem, labelAll)
          LOGGER.info("commonPortrait = " + commonPortrait)
          portraitList.add(commonPortrait)
        } else {
          throw new JobInterruptedException("不支持的透视标签类型", "the label type is not supported")
        }
      }
      val finalSql = "SELECT user_id AS device_id," + StringUtils.join(labels.map(label => ("MAX(`" + label + "`) AS " + label)).asJava, StringConstant.COMMA) + " FROM (" + StringUtils.join(portraitList, " UNION ALL ") + ") GROUP BY user_id"
      LOGGER.info("finalSql: " + finalSql)

      // 3.执行sql获取画像明细数据
      val rowDataset = spark.sql(finalSql)
      res.put(idType, rowDataset)
    } else {
      // 组合求交人群
      // 1.人群数据拆分
      val idTypes = audienceDataset.select("id_type").distinct().collectAsList()
      for(idType_row <- idTypes) {
        val idType = idType_row.getAs[String]("id_type")
        LOGGER.info("audienceDs count before: " + audienceDataset.filter("id_type = \"" + idType + "\"").count())
//        val audienceDs: Dataset[Row] = audienceDataset.filter("id_type = \"" + idType + "\"").filter("user_id IS NOT NULL")
        val audienceDs: Dataset[Row] = audienceDataset.filter("id_type = \"" + idType + "\"").filter("user_id != \"NULL\"")
        LOGGER.info("audienceDs count after: " + audienceDs.count())
        var portraitList: util.List[String] = new util.ArrayList[String]()
        var labels = new util.HashMap[String, util.ArrayList[String]]()
        for (setItem <- bean.getLabelSets) {
          val labelIdType = setItem.getIdType
          var label = labels.getOrDefault(labelIdType, new util.ArrayList[String])
          label.addAll(labelAll.get(labelIdType))
          labels.put(labelIdType, label)
          LOGGER.info("labelIdType = audienceIdType ? " + labelIdType.equals(idType))
          if (labelIdType.equals(idType)){
            if (LabelSourceEnum.custom == setItem.getLabelSource) {
              // 获取自定义标签数据集
              val jfsObjectKey = bean.getBucket + File.separator + setItem.getSourceUrl
              val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
              readconfig.setOptions(new util.HashMap[String, String]() {{put("header", "true")}})
              readconfig.setClientType(ConfigProperties.getClientType)
              readconfig.setFileType(OssFileTypeEnum.csv)
              readconfig.setObjectKey(jfsObjectKey)
              val labelDataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig)

              var audienceLabelData = labelDataset.join(audienceDs, "user_id")
              audienceLabelData.show(BigInteger.TEN.intValue())
              if (null == audienceLabelData || audienceDs.count == BigInteger.ZERO.intValue) {
                throw new JobInterruptedException("没有可用的数据", "No audience profile data")
              }
              val portraitSql = dealCustomLabelData(audienceLabelData, setItem, labelAll)
              if (StringUtils.isNotBlank(portraitSql)) {
                portraitList.add(portraitSql)
              }
            } else if (LabelSourceEnum.common.equals(setItem.getLabelSource)) {
              LOGGER.info("统一标签模型datasetId = {}", setItem.getDatasetId)
              // 多值字段和枚举字段需要拆分label_value字段
              val commonPortrait = dealCommonLabelData(spark, audienceDs, setItem, labelAll)
              LOGGER.info("commonPortrait = " + commonPortrait)
              portraitList.add(commonPortrait)
            }
            else {
              throw new JobInterruptedException("不支持的透视标签类型", "the label type is not supported")
            }
          }
        }
        LOGGER.info("portraitList.size(): " + portraitList.size())
        breakable{
          if (portraitList.size() == 0) {
            LOGGER.info("break, idType: " + idType)
            break()
          }
          val finalSql = "SELECT user_id AS device_id," + StringUtils.join(labels.get(idType).map(label => ("MAX(`" + label + "`) AS " + label)).asJava, StringConstant.COMMA) + " FROM (" + StringUtils.join(portraitList, " UNION ALL ") + ") GROUP BY user_id"
          LOGGER.info("finalSql:" + finalSql)
          val rowDataset = spark.sql(finalSql)
          res.put(idType, rowDataset)
        }
      }
    }
    res
  }

  class TagsBean(name: String, tagType: String, values: util.List[String]) {
    def getName(): String = {
      name
    }

    def getTagType: String = {
      tagType
    }

    def getValues: util.List[String] = {
      values
    }

    override def toString: String = {
      if (values.size() > 1) {
        "{\"name\":\"" + getName() + "\",\"type\":\"" + tagType + "\",\"values\":" + values.toString + "}"
      } else {
        "{\"name\":\"" + getName() + "\",\"values\":" + values.toString + "}"
      }
    }
  }
}
