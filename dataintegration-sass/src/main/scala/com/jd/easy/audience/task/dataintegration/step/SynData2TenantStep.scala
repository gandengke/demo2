package com.jd.easy.audience.task.dataintegration.step

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jd.easy.audience.common.constant.{NumberConstant, SplitDataSourceEnum, StringConstant}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.util.JsonUtil
import com.jd.easy.audience.task.commonbean.bean.SynData2TenantBean
import com.jd.easy.audience.task.commonbean.contant.{CATemplateEnum, DepartmentTypeEnum, SplitSourceTypeEnum}
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util._
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * 同步数据
 *
 * @author cdxiongmei
 * @version V1.0
 */
class SynData2TenantStep extends StepCommon[Unit] {
  private val LOGGER = LoggerFactory.getLogger(classOf[SynData2TenantStep])

  override def validate(): Unit = {
    super.validate()
    val bean = getStepBean.asInstanceOf[SynData2TenantBean]
    LOGGER.info("bean info:" + JsonUtil.serialize(bean))
    if (StringUtils.isAnyBlank(bean.getSourceDbName, bean.getSourceTableName, bean.getSubTableName)) {
      LOGGER.error("同步数据必须要指定源表和目标表前缀")
      throw new JobInterruptedException("同步数据必须要指定源表和目标表前缀", "split data must have source table and target table")
    }
  }

  override def run(map: java.util.Map[String, StepCommonBean[_]]): Unit = {
    //1.参数获取和处理
    val bean = getStepBean.asInstanceOf[SynData2TenantBean]
    val dataType = bean.getDataType
    val sourceTable = bean.getSourceDbName + StringConstant.DOTMARK + bean.getSourceTableName
    val sparkSession = SparkUtil.buildMergeFileSession(bean.getStepName)
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    LOGGER.info(s"""ready to splitdata,dataType= ${dataType},tablename=${sourceTable},subTableName= ${bean.getSubTableName}""")

    //2. 数据过滤加工
    dealDataFilter(sparkSession, bean, false)

  }

  /**
   * @description 更新数据到mysql表中
   * @param [sparkSession, bean, tableName]
   * @return void
   * @date 2022/1/11 下午3:14
   * @auther cdxiongmei
   */

  private def createTableTemplate(sparkSession: SparkSession, bean: SynData2TenantBean, selectFields: java.util.List[String]) = {
    var finalCreateSql = new StringBuilder("CREATE TABLE %s (")
    val sourceTb = bean.getSourceTableName
    sparkSession.catalog.refreshTable(sourceTb)
    val sourceCols = sparkSession.catalog.listColumns(bean.getSourceTableName).collectAsList
    LOGGER.info("表字段信息" + sourceTb + "，" + sourceCols.toString)
    import scala.collection.JavaConversions._
    for (field <- sourceCols) {
      if (selectFields.contains(field.name) && !(field.dataType.toLowerCase.contains("array") || field.dataType.toLowerCase.contains("struct") || field.dataType.toLowerCase.contains("map"))) { //添加到子表建表中
        finalCreateSql.append(field.name + " " + field.dataType)
        if (field.description != null) {
          finalCreateSql.append(" COMMENT \'" + field.description + "\'")
        }
        finalCreateSql.append(",")
      }
    }
    //新增标示字段
    finalCreateSql = new StringBuilder(finalCreateSql.substring(NumberConstant.INT_0, finalCreateSql.length - NumberConstant.INT_1))
    //表注释处理
    var tbComment: String = bean.getTableComment
    if (StringUtils.isBlank(bean.getTableComment)) {
      tbComment = sparkSession.catalog.getTable(sourceTb).description
    }
    if (StringUtils.isNotBlank(tbComment)) {
      finalCreateSql.append(")COMMENT \"" + bean.getTableComment + "\"")
    } else {
      finalCreateSql.append(")")
    }
    //分区字段处理
    if (bean.getDataType.isDesPartitioned) { //有时间分区字段
      finalCreateSql.append(" PARTITIONED BY(dt STRING) ")
    }
    finalCreateSql.append(" STORED AS ORC tblproperties('orc.compress'='SNAPPY')")
    finalCreateSql.toString
  }

  /**
   * @throws
   * @title writeDataToHive
   * @description 将拆分后的数据写入对应的hive表
   * @author cdxiongmei
   * @param: viewName
   * @param: tableName
   * @param: sparkSession
   * @param: dtField
   * @param: dataType
   * @updateTime 2021/8/9 下午7:55
   * @return: boolean
   */
  private def writeDataToHive(dataset: Dataset[Row], tableName: String, sparkSession: SparkSession, bean: SynData2TenantBean, createTemplate: String): Boolean = {
    LOGGER.info("sourceVal:" + tableName)
    dataset.show(NumberConstant.INT_10)
    var dbTable = tableName.split("\\.")
    if (!sparkSession.catalog.databaseExists(dbTable(NumberConstant.INT_0))) {
      throw new JobInterruptedException(dbTable(NumberConstant.INT_0) + "库不存在", tableName + " database is not exists")
    }
    if (!sparkSession.catalog.tableExists(dbTable(NumberConstant.INT_0), dbTable(NumberConstant.INT_1))) {
      //创建表
      SparkUtil.printAndExecuteSql(String.format(createTemplate, tableName), sparkSession)
    }
    val viewName = "tempViewTable_" + System.currentTimeMillis
    dataset.createOrReplaceTempView(viewName)
    if (!sparkSession.catalog.tableExists(tableName)) {
      LOGGER.error(tableName + "不存在")
      throw new JobInterruptedException(tableName + "不存在", tableName + " is not exists")
    }
    try {
      val sql = new StringBuilder("INSERT OVERWRITE TABLE " + tableName)
      if (bean.getDataType.isDesPartitioned) {
        sql.append(" PARTITION(dt)")
      }
      val fields = sparkSession.catalog.listColumns(tableName).map((column: Column) => column.name)(Encoders.STRING).collect().toList
      sql.append("\nSELECT " + fields.mkString(",") + " \nFROM\n" + viewName)
      LOGGER.info("insert sql:" + sql.toString)
      sparkSession.sql(sql.toString)
    } catch {
      case e: Exception =>
        LOGGER.error("writeDataToHive exception!", e)
        return false
    }
    sparkSession.catalog.dropTempView(viewName)
    true
  }

  private def getDateStrBeforeNDays(date: Date, interval: Int) = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.add(Calendar.DATE, -interval)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(cal.getTime)
  }

  /**
   *
   * 加工子表的字段信息
   *
   * @param sparkSession
   * @param bean
   * @return
   */
  private def getFieldFromSource(sparkSession: SparkSession, bean: SynData2TenantBean, isTest: Boolean): java.util.List[String] = {
    var desTbFields: java.util.List[String] = new java.util.ArrayList[String]()
    var sourceFields: List[String] = List()
    if (!isTest) {
      sparkSession.sql("use " + bean.getSourceDbName)
    }
    if (StringUtils.isBlank(bean.getFieldFilter)) {
      sourceFields = sparkSession.catalog.listColumns(bean.getSourceTableName).map((column: Column) => column.name)(Encoders.STRING).collect().toList
    } else {
      sourceFields = bean.getFieldFilter.split(StringConstant.COMMA).toList
    }
    val paramValue = SynDataFilterExpressParserEnum.parseConditions(bean.getFilterExpress)
    LOGGER.error("sourceFields:" + sourceFields + "，size=" + sourceFields.length)
    LOGGER.error("paramValue:" + paramValue.keySet())
    for (fieldItem <- sourceFields) {
      if (!paramValue.containsKey(fieldItem) && !fieldItem.equalsIgnoreCase(bean.getDtFieldSource)) {
        /**
         * 不是过滤字段，也不是时间分区字段
         */
        desTbFields.add(fieldItem)
      }
    }
    if (StringUtils.isNotBlank(bean.getDtFieldSource) && bean.getDataType.isDesPartitioned) {
      /**
       * 目标表是分区表，还需要把分区字段调整到最后一列
       */
      desTbFields.add(bean.getDtFieldSource + " AS dt")
    }
    else if (StringUtils.isBlank(bean.getDtFieldSource) && bean.getDataType.isDesPartitioned) { //将非分区表加工成分区表
      val beforeDays = BigInteger.ONE.intValue
      val buffaloTime = System.getenv("BUFFALO_ENV_NTIME")
      val jobTime = DateTime.parse(buffaloTime, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
      val statDate = getDateStrBeforeNDays(jobTime.toDate, beforeDays)
      desTbFields.add("\"" + statDate + "\" AS dt")
    }
    /**
     * 处理目标表名和部门id字段
     */
    //TODO 部门字段加工
    val deapartType: DepartmentTypeEnum = DepartmentTypeEnum.getTypeEnumBySourceType(bean.getSourceTypeEx)
    if (deapartType == null) {
      /**
       * 其他离线数据来源
       */
      desTbFields.add("\"-1\" AS department_id")
      desTbFields.add(s"""\"${bean.getDbName}.${bean.getSubTableName}\" AS table_name""")
    } else if (deapartType.equals(DepartmentTypeEnum.DATA_SOURCE)) {
      /**
       * 模版数据
       */
      val fileExpress: String = bean.getFilterExpress
      val expressCond = TableMetaUtil.parsePartitionCond(fileExpress)
      val departId: java.lang.Long = getDepartmentInfo(deapartType.getType, expressCond.get(DepartmentTypeEnum.DATA_SOURCE.getSourceField))
      if (null == departId) {
        LOGGER.error("存在脏数据，不处理，sourceType=" + deapartType + ",sourcefilter:" + fileExpress)
        desTbFields.add("\"\" AS department_id")
        desTbFields.add("\"\" AS table_name")
      } else {
        desTbFields.add("\"" + departId + "\" AS department_id")
        desTbFields.add(s"""\"${bean.getDbName}.${bean.getSubTableName}\" AS table_name""")
      }
    } else {
      /**
       * 全域广告监测数据相关，需要按部门归总数据
       */
      val sqlSource = new StringBuilder(s"""SELECT ${deapartType.getSourceField}  FROM ${bean.getSourceTableName} WHERE ${bean.getFilterExpress} GROUP BY ${deapartType.getSourceField}""")
      LOGGER.error("sqlSource:" + sqlSource.toString)
      //6. 取出源表的数据
      var sourceDS = sparkSession.sql(sqlSource.toString)

      val souceList: Array[String] = sourceDS.select(deapartType.getSourceField).as(Encoders.STRING).collect()
      var departInfoMap: Map[String, String] = Map[String, String]()
      souceList.foreach(sourceId => {
        val departId: java.lang.Long = getDepartmentInfo(deapartType.getType, sourceId)
        if (departId != null) {
          departInfoMap += (sourceId -> departId.toString)
        }
      })

      /**
       * 注册udf函数，根据源信息获取部门信息
       */
      sparkSession.udf.register("getdepartInfo", (sourceId: String, typeEx: Int) => {
        if (typeEx == 1) {
          /**
           * 获取id
           */
          departInfoMap.getOrElse(sourceId, "")
        } else {
          /**
           * 获取表名后缀
           */
          if (departInfoMap.getOrElse(sourceId, "").equalsIgnoreCase("-1")) {
            "root"
          } else {
            departInfoMap.getOrElse(sourceId, "")
          }
        }
      })
      desTbFields.add("getdepartInfo(`" + deapartType.getSourceField + "`,1) AS department_id")
      if (deapartType.equals(DepartmentTypeEnum.MONITOR_DATA)) {
        desTbFields.add("concat_ws(\"_\",\"" + bean.getDbName + "." + bean.getSubTableName + "\",getdepartInfo(`" + deapartType.getSourceField + "`,2)) AS table_name")
      } else {
        desTbFields.add("concat_ws(\"_\",\"" + bean.getDbName + "." + bean.getSubTableName + "\",`" + deapartType.getSourceField + "`) AS table_name")
      }
    }
    desTbFields
  }

  /**
   * 处理需要从源表中过滤的字段
   *
   * @param bean
   * @param sparkSession
   * @return
   */
  def dealDataFilter(sparkSession: SparkSession, bean: SynData2TenantBean, isTest: Boolean): Unit = {
    /**
     * 获取字段（包含部门和表名）
     */
    val desTbFields = getFieldFromSource(sparkSession, bean, isTest)
    LOGGER.error("after deal desTbFields:" + desTbFields)

    /**
     * 获取数据
     */
    var sourceTable = bean.getSourceTableName
    val sqlSource = new StringBuilder("SELECT " + StringUtils.join(desTbFields, ",") + " FROM " + sourceTable)
    if (StringUtils.isNotBlank(bean.getFilterExpress)) {
      /**
       * 过滤条件
       */
      var conbineFlag = " AND "
      if (!sqlSource.toString.contains("WHERE")) conbineFlag = " WHERE "
      sqlSource.append(conbineFlag + bean.getFilterExpress)
    }
    LOGGER.error("sqlSource:" + sqlSource.toString)
    //6. 取出源表的数据
    var sourceHiveData = sparkSession.sql(sqlSource.toString)
    sourceHiveData = sourceHiveData.filter("department_id !='' and table_name !=''")
    sourceHiveData.cache
    val sourceCnt = sourceHiveData.count
    LOGGER.error("sourceHiveData:" + sourceCnt)
    if (sourceCnt <= BigInteger.ZERO.intValue) {
      LOGGER.info("没有数据，无需处理")
      return
    }
    /**
     * 获取建表模版
     */
    val createTemplate = createTableTemplate(sparkSession, bean, desTbFields)

    /**
     * 按子表进行落库
     */
    val splitField = sourceHiveData.select("table_name").as(Encoders.STRING).distinct.collect
    for (sourceVal <- splitField) {
      val subData = sourceHiveData.filter("table_name=\"" + sourceVal + "\"").drop("table_name")
      LOGGER.error("write hive data, table_name:" + sourceVal)

      val tbname = sourceVal.substring(bean.getDbName.length + NumberConstant.INT_1, sourceVal.length)
      val departId: String = subData.first().getAs[String]("department_id")
      LOGGER.error("departmentId:" + java.lang.Long.valueOf(departId))
      if (isTest) {
        subData.drop("department_id").show()
        LOGGER.error("table create sql:" + String.format(createTemplate, sourceVal))
      } else {
        writeDataToHive(subData.drop("department_id"), sourceVal, sparkSession, bean, createTemplate)
        if (bean.isUpdate2Mysql) {
          TableMetaUtil.writeTableToMysql(sparkSession, bean.getTableComment, bean.getSourceTypeEx, bean.getDataSource, java.lang.Long.valueOf(departId), bean.getAccountId, bean.getAccountName, bean.getDbName, tbname)
        }
      }
    }
  }

  /**
   * 根据不同数据源信息获取对应部门信息
   *
   * @param sourceType
   * @param sourceId
   * @return
   */
  def getDepartmentInfo(sourceType: Int, sourceId: String): java.lang.Long = {
    //TODO 调用接口获取部门信息
    LOGGER.error("getDepartmentInfo=> 获取部门信息，sourceType={},sourceId={}", sourceType, sourceId)
    val bodyInfo = new JSONObject
    bodyInfo.put("id", sourceId)
    bodyInfo.put("type", sourceType)
    val urlStr = ConfigProperties.SERVICE_URI + "/ea/api/cdp/datasource/queryDepartmentByIdAndType"
    LOGGER.error("ca url:" + urlStr + ", bodyInfo:" + bodyInfo.toString)
    val client = new DefaultHttpClient
    var response: CloseableHttpResponse = null
    val post = new HttpPost(urlStr)
    post.setHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(JSON.toJSONString(bodyInfo, SerializerFeature.PrettyFormat)))
    response = client.execute(post)
    val ret = EntityUtils.toString(response.getEntity, "UTF-8")
    LOGGER.error("body=" + bodyInfo.toString() + ",return=" + ret)
    val jsonObject = JSON.parseObject(ret)
    if ("0" == jsonObject.getString("status")) {
      LOGGER.error("成功，系统处理正常")
      val departId = jsonObject.getLong("result")
      if (null == departId || departId < NumberConstant.N_INT_1) {
        NumberConstant.N_LONG_1
      } else {
        departId
      }
    } else {
      //脏数据问题
      LOGGER.error("脏数据，不处理，bodyInfo=" + bodyInfo)

      /**
       *
       * 测试使用
       */
//      var departMap: Map[String, java.lang.Long]  = Map("2495933136764928"-> 20)
//      departMap += ("2496456003944448"-> 20L)
//      departMap += ("2496455880212481"-> 21L)
//      departMap += ("2496454793887745"-> 22L)
//      departMap += ("2495948298125312"-> 23L)
//      departMap += ("2495952367648769"-> 23L)
//      departMap += ("2495949801783296"-> 27L)
//      departMap += ("475180"-> 23L)
//      departMap += ("122352"-> 40L)
//      departMap.getOrElse(sourceId, null)
      null
    }
  }

}
