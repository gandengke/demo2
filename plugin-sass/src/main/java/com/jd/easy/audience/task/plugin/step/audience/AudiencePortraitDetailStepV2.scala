package com.jd.easy.audience.task.plugin.step.audience

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.parser.Feature
import com.fasterxml.jackson.databind.ObjectMapper
import com.jd.easy.audience.common.constant.{LabelSourceEnum, LabelTypeEnum, NumberConstant, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.commonbean.bean.{AudiencePortraitProfileBean, CommonLabelSetInfolBean}
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil
import com.jd.easy.audience.task.plugin.util.DbManager
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils.{INDEX_NOT_FOUND, escape}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, functions}
import org.slf4j.LoggerFactory

import java.io.File
import java.math.BigInteger
import java.net.URI
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, HashMap, Properties}
import scala.collection.JavaConversions.{asScalaBuffer, asScalaSet, mapAsScalaMap}
import scala.collection.JavaConverters._

/**
 * 360画像任务加工
 *
 * @author cdxiongmei
 * @date 2022/2/15 下午3:01
 * @version V1.0
 */
class AudiencePortraitDetailStepV2 extends StepCommon[util.Map[String, AnyRef]] {
  private val LOGGER = LoggerFactory.getLogger(classOf[AudiencePortraitDetailStepV2])
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val UPDATE_LIMITNUM = NumberConstant.INT_40

  private val AUDIENCE_PORTRAIT_LABEL_TB = "ea_audience_portrait_label"
  private val LABEL_VALUE_MAPPING_TB = "ea_label_value_mapping"
  private val LABEL_MAPPING_TB = "ea_label_mapping"

  private val LABELVAL_MAPPING_FIELD = "name,description,label_name,create_time,update_time,is_deleted"
  private val LABLE_MAPPING_FIELD = "name,description,data_from,tab,tab_value,type,is_default,is_rotate,update_time,is_deleted,label_type"

  override def run(dependencies: util.Map[String, StepCommonBean[_]]) = {

    // 1.环境准备
    val sparkSession: SparkSession = SparkLauncherUtil.buildSparkSession("AudiencePortraitDetailStep")
    val bean: AudiencePortraitProfileBean = getStepBean.asInstanceOf[AudiencePortraitProfileBean]
    val updateTime = new Date
    val curDate = SIMPLE_DATE_FORMAT.format(updateTime)

    // 2.标签信息处理
    val tagsAll = labelInit(sparkSession, bean.getLabelSets, curDate)

    // 3.保存人群画像明细
    var output: util.List[util.Map[String, AnyRef]] = dealLabelData(sparkSession, bean, tagsAll)

    // 4.写入标签和标签枚举信息
    writeAudiencePortraitLabels(output, bean, curDate)

    val outputMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    outputMap.put("data", output)
    LOGGER.info("outputMap：" + outputMap.toString)
    bean.setOutputMap(outputMap)
    outputMap
  }

  /**
   * @description 功能描述: 保存数据到ck表中
   * @author cdxiongmei
   * @date 2022/4/21 11:56 AM
   * @param
   * @return
   */
  def save2ClickHouseTable(dataDf: DataFrame, portraitId: String, idType: String): String = {
    dataDf.printSchema()
    dataDf.show(10)
    val jdbcUrl = s"""jdbc:clickhouse://${ConfigProperties.CK_URL}"""
    val databaseName = ConfigProperties.CK_DBNAME
    val user = ConfigProperties.CK_USER
    val password = ConfigProperties.CK_PASS
    val socketTimeout=ConfigProperties.CK_SOCKET_TIMEOUT
    val tableName = s"""${ConfigProperties.CK_TABLEPREFIX}${idType.toLowerCase().replaceAll("-", "")}_${portraitId}${ConfigProperties.CK_TABLESUFFIX}"""
    val columns = dataDf.columns.map(c => {
      if (c.equals("device_id")) {
        c + " String"
      } else {
        c + " Int32"
      }
    }).mkString(",")
    //    val ckDriver = "com.github.housepower.jdbc.ClickHouseDriver"
    val ckDriver = "ru.yandex.clickhouse.ClickHouseDriver"
    Class.forName(ckDriver) //指定连接类型
    val jdbcSocketTimeOut = jdbcUrl + "/" + databaseName + "?socket_timeout=" + socketTimeout
    LOGGER.info("jdbcSocketTimeOut: " + jdbcSocketTimeOut)
    val conn = DriverManager.getConnection(jdbcSocketTimeOut, user, password)
    val statement = conn.createStatement()
    val localCreateSql =
      s"""
         |CREATE
         |	TABLE IF NOT EXISTS ${databaseName}.${tableName}_local ON CLUSTER default
         |	(
         |		${columns}
         |   )
         |ENGINE = ReplicatedMergeTree
         |	(
         |		'/clickhouse/tables/{shard}/${databaseName}/${tableName}_local',
         |		'{replica}'
         |	)
         |ORDER BY
         |	(
         |		device_id
         |	) ;
         |
         |""".stripMargin
    LOGGER.info("localCreateSql:" + localCreateSql)
    statement.execute(localCreateSql)
    statement.execute(
      s"""
         |CREATE
         |	TABLE IF NOT EXISTS ${databaseName}.${tableName} ON CLUSTER default AS ${databaseName}.${tableName}_local ENGINE = Distributed
         |	(
         |		default,
         |		${databaseName},
         |		${tableName}_local,
         |		rand()
         |	);
         |
         |""".stripMargin)
    LOGGER.info("Successfully created ck table!")
    statement.execute(
      s"""
         |ALTER TABLE ${databaseName}.${tableName}_local ON CLUSTER default drop partition tuple();
         |""".stripMargin)
    LOGGER.info("Successfully truncate ck table!")
    statement.close()
    conn.close()
    //准备写入数据
    val prop = new Properties()
    prop.put("driver", ckDriver)
    prop.put("user", user)
    prop.put("password", password)
    val writerview = System.currentTimeMillis()
    dataDf.write.mode(SaveMode.Append)
      .option("batchsize", "100000")
      .option("isolationLevel", "NONE")
      .option("numPartitions", "5")
      .jdbc(s"""${jdbcUrl}/${databaseName}?socket_timeout=${socketTimeout}""", s"""${databaseName}.${tableName}_local""", prop)
    val endStview = System.currentTimeMillis()
    println(s"save2ckTable cost time ${endStview - writerview}")
    tableName
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
  //  def writeAudiencePortraitLabels(labels: util.List[String], tags: String, bean: AudiencePortraitProfileBean, updateTime: String): Unit = {
  //    val insertSql = "UPDATE " + AUDIENCE_PORTRAIT_LABEL_TB +
  //      " SET portrait_url=\"" + bean.getOutputPath + File.separator + AUDIENCE_PORTRAIT_DETAIL_FILENAME +
  //      "\", tags=\"" + tags +
  //      "\", labels=\"" + StringUtils.join(labels, StringConstant.COMMA) +
  //      "\", update_time=\"" + updateTime +
  //      "\" WHERE is_deleted = 0 AND audience_id = " + bean.getAudienceId + " AND portrait_id = " + bean.getPortraitId
  //
  //    // 写入数据
  //    updateSql(insertSql)
  //  }

  /**
   * 画像mysql表中标签信息，表信息维护
   *
   * @param output
   * @param bean
   * @param updateTime
   */
  def writeAudiencePortraitLabels(output: util.List[util.Map[String, AnyRef]], bean: AudiencePortraitProfileBean, updateTime: String): Unit = {
    var labels: scala.List[String] = scala.List[String]()
    bean.getLabelSets.map { set =>
      labels = labels ++: set.getLabelValueDetails.map(label => (LabelSourceEnum.getLabelPrefix(set.getLabelSource) + label.getLabelId)).toList
    }
    val ckTables: scala.List[String] = output.map(t => t.get("path").toString).toList
    val mapper = new ObjectMapper()
    val tags: String = escape(mapper.writeValueAsString(output))
    LOGGER.info("output:" + output.toString)
    LOGGER.info("tags:" + tags)
    val insertSql = "UPDATE " + AUDIENCE_PORTRAIT_LABEL_TB +
      " SET portrait_url=\"" + ckTables.mkString(StringConstant.COMMA) +
      "\", tags=\"" + tags +
      "\", labels=\"" + labels.mkString(StringConstant.COMMA) +
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
   * @return [user_id, id_type]
   */
  def getAudienceData(spark: SparkSession, bean: AudiencePortraitProfileBean): Dataset[Row] = {

    // 1.jsf协议的oss路径
    val jfsPath = bean.getBucket + File.separator + URI.create(bean.getAudienceDataFile).getPath
    LOGGER.info("jfsPath is : " + jfsPath)

    // 2.读取oss数据
    val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
    readconfig.setOptions(new HashMap[String, String]() {
      {
        put("header", "true")
      }
    })
    readconfig.setClientType(ConfigProperties.getClientType)
    readconfig.setFileType(OssFileTypeEnum.orc)
    readconfig.setObjectKey(jfsPath)

    var dataset: Dataset[Row] = null
    val audienceData = JdCloudOssUtil.readDatasetFromOss(spark, readconfig)
    val cnt = audienceData.columns.size
    if (cnt == NumberConstant.INT_1) {
      dataset = audienceData.toDF("user_id").withColumn("id_type", functions.lit(bean.getLabelSets.get(NumberConstant.INT_0).getIdType))
    } else {
      dataset = audienceData.toDF("user_id", "id_type").selectExpr("user_id", "UPPER(id_type) AS id_type")
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
  def labelInit(sparkSession: SparkSession, labels: util.List[CommonLabelSetInfolBean], updateTime: String): util.Map[String, util.List[JSONObject]] = {

    // label_mapping数据写入sql组装
    var labelInsertSqlList: util.List[String] = new util.ArrayList[String]
    // 注册udf函数时使用，用于标签枚举的映射
    var labelInfo: scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, Long]] = Map[String, scala.collection.immutable.Map[String, Long]]()
    // 封装tags输出到output中
    var tagsAll: util.Map[String, util.List[JSONObject]] = new util.HashMap[String, util.List[JSONObject]]()

    for (setItem <- labels) {
      val idType = setItem.getIdType
      if (!LabelSourceEnum.custom.equals(setItem.getLabelSource) && !LabelSourceEnum.common.equals(setItem.getLabelSource)) {
        throw new JobInterruptedException("无法识别该标签类型", "The label type was not recognized!")
      }
      setItem.getLabelValueDetails.foreach(label => {
        val tagHeaderName: String = LabelSourceEnum.getLabelPrefix(setItem.getLabelSource) + label.getLabelId;
        val labelType = label.getLabelType.getValue
        labelInsertSqlList.add("(\"" + tagHeaderName + "\",\"" +
          label.getLabelName + "\",3,\"" +
          label.getTab + "\",\"" +
          label.getTabValue + "\",\"" +
          label.getType + "\",\"" +
          label.getIsDefault + "\",\"" +
          label.getIsRotate + "\",\"" +
          updateTime + "\", 0, \"" +
          labelType + "\")"
        )
        if (labelInsertSqlList.size() >= UPDATE_LIMITNUM) {
          insertMappingSql(labelInsertSqlList, LABLE_MAPPING_FIELD, LABEL_MAPPING_TB, true)
          labelInsertSqlList.clear()
        }

        // value_mapping数据写入
        val valueMappingWrite: util.Map[java.lang.Long, String] = new util.HashMap[java.lang.Long, String]()

        if (labelType.equals(LabelTypeEnum.multitype.getValue)) {
          var i: Long = NumberConstant.LONG_0
          label.getValueMapInfo.foreach {
            valueInfo => {
              valueMappingWrite.put(scala.math.pow(NumberConstant.INT_2, i).toLong, valueInfo._2)
              i = i + NumberConstant.INT_1
            }
          }
          valueMappingWrite.put(NumberConstant.LONG_0, "其他")
        } else {
          valueMappingWrite.putAll(label.getValueMapInfo)
          valueMappingWrite.put(NumberConstant.N_LONG_1, "其他")
        }
        writePortraitValues(valueMappingWrite, tagHeaderName, updateTime)

        val tagsAllId: util.List[JSONObject] = tagsAll.getOrDefault(idType, new util.ArrayList[JSONObject])
        tagsAllId.add(com.alibaba.fastjson.JSON.parseObject(new TagsBean(tagHeaderName, labelType, valueMappingWrite.keySet().toList.asJava).toString, Feature.OrderedField));
        tagsAll.put(idType, tagsAllId)

        labelInfo += (tagHeaderName -> valueMappingWrite.map(t => (t._2, t._1.toLong)).toMap)
      })

    }
    // label_mapping数据写入
    if (labelInsertSqlList.size() > NumberConstant.INT_0) {
      insertMappingSql(labelInsertSqlList, LABLE_MAPPING_FIELD, LABEL_MAPPING_TB, true)
    }
    LOGGER.info("labelInfo：" + labelInfo.toString)
    // 注册udf函数，进行标签枚举值的映射
    sparkSession.udf.register("getLabelValueCode", (labelName: String, value: String, labelType: String) => {
      if (labelType.equals(LabelTypeEnum.multitype.getValue)) {
        if (StringUtils.isAnyBlank(value, labelName)) {
          NumberConstant.LONG_0
        } else {
          value.split(File.separator).map(t => labelInfo.getOrElse(labelName, Map[String, Long]()).getOrElse(t, 0L)).sum
        }
      } else {
        if (StringUtils.isAnyBlank(value, labelName)) {
          NumberConstant.N_LONG_1
        } else {
          labelInfo.getOrElse(labelName, Map[String, Long]()).getOrElse(value, -1L)
        }
      }
    })
    tagsAll
  }

  /**
   * 计算统一标签模型的画像明细数据
   *
   * @param sparkSession
   * @param audienceDataset
   * @param setItem
   * @return
   */
  def dealCommonLabelData(sparkSession: SparkSession, audienceDataset: Dataset[Row], setItem: CommonLabelSetInfolBean, labelTypes: util.HashMap[String, String]): String = {

    // 统一标签模型展开
    val labelSelect = new util.ArrayList[String]
    setItem.getLabelValueDetails.map(
      label => {
        labelSelect.add(LabelSourceEnum.getLabelPrefix(LabelSourceEnum.common) + label.getLabelId)
      })

    // 获取统一标签数据集数据并对人群数据集打标
    var commonLabelData = sparkSession.sql(
      s"""
         |SELECT
         |	UPPER(user_id) AS user_id,
         |	label_name,
         |	label_value
         |FROM
         |	${setItem.getSourceUrl} WHERE set_id = '${setItem.getSetId}'
         |""".stripMargin)
    commonLabelData = commonLabelData.join(audienceDataset, "user_id")
    commonLabelData.cache()
    if (null == commonLabelData || commonLabelData.count == BigInteger.ZERO.intValue) {
      return StringConstant.EMPTY
    }
    var unionList: util.List[String] = new util.ArrayList[String]
    for (i <- NumberConstant.INT_0 until setItem.getLabelValueDetails.size) {
      val labelEle = setItem.getLabelValueDetails.get(i)
      val labelType = labelEle.getLabelType.getValue
      val tagHeaderName = LabelSourceEnum.getLabelPrefix(LabelSourceEnum.common) + labelEle.getLabelId
      commonLabelData.filter("label_name = \"" + labelEle.getLabelName + "\"").createOrReplaceTempView("commontempview_" + labelEle.getLabelId)
      var tempSelectExpr: util.List[String] = new util.ArrayList[String]
      labelSelect.foreach(ele => tempSelectExpr.add("\"-1\" AS " + ele))
      LOGGER.info("getLabelValueCode tagname=" + tagHeaderName + ",labelType=" + labelType)
      tempSelectExpr.set(i, "getLabelValueCode(\"" + tagHeaderName + "\", `label_value`, \"" + labelType + "\") AS " + tagHeaderName)
      val sqlUnion = "SELECT user_id," + StringUtils.join(tempSelectExpr, StringConstant.COMMA) + " FROM commontempview_" + labelEle.getLabelId
      unionList.add(sqlUnion)
    }

    val fields = new util.ArrayList[String]
    fields.addAll(labelTypes.keySet())

    val maxExpress = fields.map(label => {
      if (labelSelect.contains(label)) {
        "MAX(`" + label + "`) AS " + label
      } else {
        if (labelTypes.get(label).equals(LabelTypeEnum.multitype.getValue)) {
          "\"" + NumberConstant.INT_0 + "\" AS " + label
        }
        else {
          "\"" + NumberConstant.N_INT_1 + "\" AS " + label
        }
      }
    }).toList.asJava

    "SELECT user_id," + StringUtils.join(maxExpress, StringConstant.COMMA) + " FROM (" + StringUtils.join(unionList, " UNION ALL ") + ") GROUP BY user_id"
  }

  /**
   * 计算自定义标签数据的画像明细,拼装sql
   *
   * @param audienceLabelData
   * @param setItem
   * @param labelTypes [tag_11->multi,tag_22->enum]
   * @return
   */
  def dealCustomLabelData(spark: SparkSession, audienceDs: Dataset[Row], labelSet: CommonLabelSetInfolBean, bean: AudiencePortraitProfileBean, labelTypes: util.HashMap[String, String]): String = {
    // 1.获取自定义标签数据集
    val jfsObjectKey = bean.getBucket + File.separator + labelSet.getSourceUrl
    val readconfig: JdCloudDataConfig = new JdCloudDataConfig(bean.getEndpoint, bean.getAccessKey, bean.getSecretKey)
    readconfig.setOptions(new util.HashMap[String, String]() {
      {
        put("header", "true")
      }
    })
    readconfig.setClientType(ConfigProperties.getClientType)
    readconfig.setFileType(OssFileTypeEnum.csv)
    readconfig.setObjectKey(jfsObjectKey)
    val labelDataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig)
      .withColumnRenamed("user_id", "user_id_old").withColumn("user_id", functions.upper(functions.col("user_id_old"))).drop("user_id_old")
    // 2.对人群数据集打标
    var audienceLabelData = audienceDs.join(labelDataset, Seq("user_id"), "inner")
    audienceLabelData.printSchema()
    audienceLabelData.cache

    if (audienceLabelData.count <= NumberConstant.INT_0) {
      LOGGER.info("join with audience result count = 0")
      StringConstant.EMPTY
    } else {
      audienceLabelData.show(BigInteger.TEN.intValue)
      audienceLabelData.createOrReplaceTempView("customview_" + labelSet.getDatasetId)

      // 3.解析标签的tag信息（将多值的字段进行展开）
      val labelIds = labelSet.getLabelValueDetails.map(label => LabelSourceEnum.getLabelPrefix(LabelSourceEnum.custom) + label.getLabelId)
      val fieldInfos = new util.ArrayList[String]()
      fieldInfos.addAll(labelTypes.keySet())
      for (i <- NumberConstant.INT_0 until fieldInfos.size) {
        val labelType = labelTypes.get(fieldInfos.get(i))
        LOGGER.info("getLabelValueCode tagname=" + fieldInfos.get(i) + ",labelType=" + labelType)
        if (labelIds.contains(fieldInfos.get(i))) {
          fieldInfos.set(i, "getLabelValueCode(\"" + fieldInfos.get(i) + "\",`" + fieldInfos.get(i) + "`, \"" + labelType + "\") AS " + fieldInfos.get(i))
        } else {
          if (labelType.equals(LabelTypeEnum.enumtype.getValue))
            fieldInfos.set(i, "\"-1\" AS " + fieldInfos.get(i))
          else
            fieldInfos.set(i, "\"0\" AS " + fieldInfos.get(i))
        }
      }
      "SELECT user_id," + StringUtils.join(fieldInfos, StringConstant.COMMA) + " FROM customview_" + labelSet.getDatasetId
    }
  }

  /**
   * 获取不同标示类型下标签数据集标签画像明细数据
   *
   * @param spark
   * @param bean
   * @return
   */
  def dealLabelData(spark: SparkSession, bean: AudiencePortraitProfileBean, tagsAll: util.Map[String, util.List[JSONObject]]): util.List[util.Map[String, AnyRef]] = {

    import scala.collection.JavaConversions._
    var output: util.List[util.Map[String, AnyRef]] = new util.ArrayList[util.Map[String, AnyRef]]
    // 1.加载人群数据[user_id, id_type]
    val audienceDataset: Dataset[Row] = getAudienceData(spark, bean).cache

    // 2.从数据集列表中获取id_type列表（人群包中可能会多，以数据集中类型为准）
    val idTypes = bean.getLabelSets.map(set => set.getIdType).toList.distinct
    for (idTypeRow <- idTypes) {
      LOGGER.info("idType: " + idTypeRow)
      val audienceDs: Dataset[Row] = audienceDataset.filter("id_type = \"" + idTypeRow + "\"")
      val ckTableName = getDatasetDataSqlByidtype(spark, bean.getLabelSets.filter(setItem => setItem.getIdType.toLowerCase().equals(idTypeRow.toLowerCase())), bean, audienceDs)
      if (StringUtils.isNotBlank(ckTableName)) {
        val outputMapCombine: util.Map[String, AnyRef] = new util.LinkedHashMap[String, AnyRef]
        // 6.返回值封装
        outputMapCombine.put("path", ckTableName)
        outputMapCombine.put("pathType", "ck")
        outputMapCombine.put("tags", tagsAll.get(idTypeRow))
        output.add(outputMapCombine)
      }
    }
    if (output.isEmpty) {
      throw new JobInterruptedException("该人群包的画像数据为空", "No such audience portrait data")
    }
    output
  }

  /**
   * 获取同用户标示类型数据集的标签sql语句（包含该用户标示下的所有标签）
   *
   * @param spark
   * @param labelSet   进行画像分析的数据集信息
   * @param bean       参数配置信息
   * @param audienceDs 进行画像的人群信息
   * @param labelTypes
   * @return
   */
  def getDatasetDataSqlByidtype(spark: SparkSession, setList: util.List[CommonLabelSetInfolBean], bean: AudiencePortraitProfileBean, audienceDataset: Dataset[Row]): String = {
    val audienceDs: Dataset[Row] = audienceDataset.filter("user_id IS NOT NULL AND user_id != 'NULL'")
    audienceDs.cache()
    LOGGER.info("audienceDs count after filter: " + audienceDs.count())
    var portraitList: util.List[String] = new util.ArrayList[String]()
    val labelTypes: util.HashMap[String, String] = new util.HashMap[String, String]()
    setList.map { set =>
      labelTypes.putAll(set.getLabelValueDetails.map(label => (LabelSourceEnum.getLabelPrefix(set.getLabelSource) + label.getLabelId, label.getLabelType.getValue)).toMap.asJava)
    }
    for (setItem <- setList) {
      // 按数据集进行处理
      var portraitSql = ""
      if (LabelSourceEnum.custom == setItem.getLabelSource) {
        LOGGER.info("自定义标签模型datasetId = {}", setItem.getDatasetId)
        portraitSql = dealCustomLabelData(spark, audienceDs, setItem, bean, labelTypes)
      } else if (LabelSourceEnum.common.equals(setItem.getLabelSource)) {
        LOGGER.info("统一标签模型datasetId = {}", setItem.getDatasetId)
        // 多值字段和枚举字段需要拆分label_value字段
        portraitSql = dealCommonLabelData(spark, audienceDs, setItem, labelTypes)
      }
      else {
        throw new JobInterruptedException("不支持的透视标签类型", "the label type is not supported")
      }
      if (StringUtils.isNotBlank(portraitSql)) {
        portraitList.add(portraitSql)
      }
    }
    LOGGER.info("portraitList.size(): " + portraitList.size())
    if (CollectionUtils.isEmpty(portraitList)) {
      return StringConstant.EMPTY
    }
    // 该标签类型和标签加密类型下的所有标签
    val finalSql = "SELECT user_id AS device_id," + StringUtils.join(labelTypes.keySet().map(label => ("MAX(`" + label + "`) AS " + label)).asJava, StringConstant.COMMA) + " FROM (" + StringUtils.join(portraitList, " UNION ALL ") + ") GROUP BY user_id"
    LOGGER.info("finalSql: " + finalSql)
    val rowDataset = spark.sql(finalSql)
    if (null == rowDataset || rowDataset.count() == NumberConstant.INT_0) {
      return StringConstant.EMPTY
    }
    val ckTableName = save2ClickHouseTable(rowDataset, bean.getPortraitId, setList.get(NumberConstant.INT_0).getIdType)
    ckTableName
  }

  /**
   *
   * @param spark
   * @param labelSet
   * @param bean
   * @param audienceDs
   * @param labelTypes
   * @return
   */

  class TagsBean(name: String, tagType: String, values: util.List[java.lang.Long]) {
    def getName(): String = {
      name
    }

    def getTagType: String = {
      tagType
    }

    def getValues: util.List[java.lang.Long] = {
      values
    }

    override def toString: String = {
      "{\"name\":\"" + getName() + "\",\"type\":\"" + tagType + "\",\"values\":" + values.toString + "}"
    }

  }
}
