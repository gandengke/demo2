package com.jd.easy.audience.task.dataintegration.property

import com.jd.easy.audience.common.constant.NumberConstant

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.oss.OssClientTypeEnum
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.StringUtils

/**
 * @title ConfigProperties
 * @description 全局配置属性
 * @author cdxiongmei
 * @updateTime 2021/8/9 下午9:12
 * @throws
 */
object ConfigProperties extends Serializable {

  private val properties = getProperties
  val templateInfo: Config = getTemplate
  //应用端链接
  val SERVICE_URI: String = properties.getProperty("SERVICE_URI")
  //对象存储协议类型
  val JSS_MODE: String = properties.getProperty("JSS_MODE")
  //数据库名称
  val JED_DBNAME: String = properties.getProperty("JED_DBNAME")
  //数据库域名
  val JED_URL: String = properties.getProperty("JED_URL")
  //数据库用户名
  val JED_USER: String = properties.getProperty("JED_USER")
  //数据库密码
  val JED_PASSWORD: String = new String(Base64.getDecoder.decode(properties.getProperty("JED_PASSWORD")), StandardCharsets.UTF_8)
  //ca数据拆分模版更新日志表
  val MARK_TABLE: String = properties.getProperty("MARK_TABLE")
  //cdp拆分结果日志
  val CDPLOG_TABLE: String = properties.getProperty("CDPLOG_TABLE")
  //dtshdfs path
  val DTS_HDFS_PATH: String = properties.getProperty("TARGET_BASE_PATH")
  val MODULE_DB_NAME: String = properties.getProperty("MODULE_DB_NAME")
  //  /**
  //   * hdfs保存营销回执明细
  //   */
  private val WRITER_BATCH_PATH_MODULE: String = properties.getProperty("WRITER_BATCH_PATH")
  //  /**
  //   * 营销回执明细
  //   */
  //  val MARKETING_DETAIL_TB: String = properties.getProperty("MARKETING_DETAIL")
  val TABLE_INFO_TB: String = "ea_create_table_info"
  val CDP_TABLE_PREFIX: String = "adm_"
  val BDP_USER: String = "USER"
  val HADOOP_USER: String = "HADOOP_USER_NAME"
  val BUFFALO_ENV_NTIME: String = "BUFFALO_ENV_NTIME"
  val BUFFALO_ENV_BCYCLE: String = "BUFFALO_ENV_BCYCLE" //2021-09-16-21-15
  val BUFFALO_ENV_CYCLE_TYPE: String = "BUFFALO_ENV_CYCLE_TYPE" //minute,hour,day,week
  val BUFFALO_ENV_YDAY: String = "BUFFALO_ENV_YDAY" //系统前一天时间（日期）yyyyMMdd
  val PUBLIC_DB_NAME: String = "ae_public"
  val PUBLIC_DB_NAME_DB: String = "ae_public.db"
  val DATABASE_URI: String = "hdfs://ns1/user/cdp_biz-org.bdp.cs"
  val BUILD_SPLIT_BEANS_CLASS: String = "buildBeansClass"
  val FILTER_PATITION_FIELD: String = "isPartition = 'true'"
  val FILTER_ID_NOT_NULL: String = "id is not null"
  val FIELD_ID: String = "id"
  val FIELD_ACCOUNT_ID: String = "account_id"
  val FIELD_MAIN_ACCOUNT_NAME: String = "main_account_name"
  val FIELD_SUB_ACCOUNT_NAME: String = "sub_account_name"
  val FIELD_DATABASE_NAME: String = "database_name"
  val FIELD_TABLE_DATA_UPDATE_TIME: String = "table_data_update_time"
  val FIELD_TABLE_NAME: String = "table_name"
  val FIELD_TABLE_COMMENT_NEW: String = "table_comment_new"
  val FIELD_COL_NAME: String = "col_name"
  val FIELD_LOCATION: String = "Location"
  val FIELD_DATA_TYPE: String = "data_type"
  val DTS_FIELD_TABLENAME: String = "@tableName@"
  val DATEFORMATE_YYYMMDD_HH: String = "yyyy-MM-dd/HH"
  val DATEFORMATE_YYYMMDD_HH_MM: String = "yyyy-MM-dd/HH/mm"
  val DATEFORMATE_YYYMMDD: String = "yyyy-MM-dd"
  val DATEFORMATE_TIMESTAMP: String = "yyyy-MM-dd HH:mm:ss"
  val UPDATE_LIMITNUM: Int = NumberConstant.INT_40



  private def getProperties: Properties = {
    val properties: Properties = new Properties
    val in: InputStream = getClass.getResourceAsStream(s"/config.properties")
    properties.load(new BufferedInputStream(in))
    properties
  }

  private def getTemplate: Config = {
    ConfigFactory.parseResources(s"template.conf")
  }

  def getClientType: OssClientTypeEnum = {
    if (StringUtils.isBlank(JSS_MODE)) throw new JobInterruptedException("没有配置OSS 协议类型", "The value of JSS_Mode is not exist!")
    val clientType = OssClientTypeEnum.valueOf(JSS_MODE)
    if (null == clientType) throw new JobInterruptedException("配置OSS 协议类型不合法", "The value of JSS_Mode is illegal！")
    clientType
  }

  def getHdfsPath(bdpUser: String): String = {
    String.format(WRITER_BATCH_PATH_MODULE, bdpUser)
  }

  def getCreateTableModule(dbName: String, tableName: String): String = {
    val in: InputStream = getClass.getResourceAsStream(s"/createTableModule/" + tableName)
    val br = new BufferedReader(new InputStreamReader(in))
    var result = new StringBuilder
    result = result.append(Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n"))
    String.format(result.toString(), dbName)
  }


}
