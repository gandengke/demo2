package com.jd.easy.audience.task.plugin.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.common.model.param.{DatasetUserIdConf, RelDatasetModel}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import java.io.{IOException, UnsupportedEncodingException}
import java.util
import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConverters.asScalaBufferConverter

object CommonDealUtils extends Serializable {
  private val LOGGER = LoggerFactory.getLogger(CommonDealUtils.getClass)

  /**
   * 解析分隔符
   *
   * @param fieldDelimter
   * @return
   */
  def parseFieldDelimter(fieldDelimiter: String) = {
    val replaceChar = "\\\\"
    val charSpecial = Array("$", "(", ")", "*", "+", ".", "[", "]", "?", "\\", "/", "^", "{", "}", "|")
    val charArr = fieldDelimiter.toCharArray
    val fieldDelimiterNew = charArr.map {
      char =>
        if (charSpecial.contains(char.toString)) {
          replaceChar + char.toString
        } else {
          char.toString
        }
    }.mkString("")
    fieldDelimiterNew
  }

  /**
   * 通过接口获取数据源的oneid配置信息
   *
   * @param relDataset 数据来源
   * @param useridTable 用户标示来源表（eg:ae_test.tablename_59）
   * @return 对于没有oneid配置信息的数据源，接口不会返回该表信息
   * @throws
   */
  def getSourceOneIdConfig(relDataset: util.List[RelDatasetModel], useridTable: String): util.HashMap[String, util.List[DatasetUserIdConf]] = {
    var userDbName: String = "";
    var userTbName: String = ""
    if (StringUtils.isNotBlank(useridTable)) {
       userDbName = useridTable.split("\\.")(0)
       userTbName = useridTable.split("\\.")(1)
    }
    val config: util.HashMap[String, util.List[DatasetUserIdConf]] = new util.HashMap[String, util.List[DatasetUserIdConf]]
    //TODO 接口验证
    val sourceInfos: util.List[JSON] = new util.ArrayList[JSON]
    val urlStr: String = ConfigProperties.SERVICE_URI + "/ea/api/cdp/ca/tableConfigList"
    /**
     * 过滤需要oneid配置信息的数据源
     */
    val needOneIdSource = relDataset.asScala.filter(relsource => relsource.isUseOneId || (relsource.getDatabaseName.equalsIgnoreCase(userDbName) && relsource.getDataTable.equalsIgnoreCase(userTbName)))
    needOneIdSource.map(source => {
      val bodyInfo: JSONObject = new JSONObject
      bodyInfo.put("dbName", source.getDatabaseName)
      bodyInfo.put("tableName", source.getDataTable)
      sourceInfos.add(bodyInfo)
    })
    LOGGER.info("sourceconfig url:" + urlStr + ", sourceInfo:" + sourceInfos.toString)
    val client: DefaultHttpClient = new DefaultHttpClient
    var response: HttpResponse = null
    val post: HttpPost = new HttpPost(urlStr)
    post.setHeader("Content-Type", "application/json")
    var jsonRet: JSONObject = new JSONObject
    try {
      post.setEntity(new StringEntity(JSON.toJSONString(sourceInfos, SerializerFeature.PrettyFormat)))
      response = client.execute(post)
      jsonRet = JSON.parseObject(EntityUtils.toString(response.getEntity, "UTF-8"))
      LOGGER.info("body=" + sourceInfos.toString + ",return=" + jsonRet.toString)
    } catch {
      case e: UnsupportedEncodingException =>
        throw new RuntimeException(e)
      case e: ClientProtocolException =>
        throw new RuntimeException(e)
      case e: IOException =>
        throw new RuntimeException(e)
    }
    if (!jsonRet.isEmpty && "0".equalsIgnoreCase(jsonRet.getString("status"))) {
      LOGGER.info("成功，系统处理正常")
      val result: JSONArray = jsonRet.getJSONArray("result")
      if (null == result || result.isEmpty || result.toArray().size != needOneIdSource.size) {
        LOGGER.error("数据源无可用oneid信息")
        throw new JobInterruptedException("数据源oneid信息配置错误", "Your datasource's oneid configure occur error")
      }
      result.toArray.foreach(obj => {
        val configInfo: JSONObject = JSON.parseObject(obj.toString)
        val tableName: String = configInfo.get("table").toString
        val idList: JSONArray = configInfo.getJSONArray("idList")
        val  adderConfig= new java.util.function.Function[String, util.List[DatasetUserIdConf]]() {
          override def apply(t: String): util.List[DatasetUserIdConf] = new util.ArrayList[DatasetUserIdConf]()
        }
        val configList: util.List[DatasetUserIdConf] = config.computeIfAbsent(tableName, adderConfig)
        idList.toArray().foreach(configItem =>{
          val configItemJson: JSONObject = JSON.parseObject(configItem.toString)
          val confEle: DatasetUserIdConf = new DatasetUserIdConf
          confEle.setUserField(configItemJson.getString("fieldName"))
          confEle.setUserIdType(configItemJson.getLong("typeId"))
          configList.add(confEle)
        })
      }
      )
    }

    else {
      LOGGER.error("数据源oneid配置接口错误")
      throw new JobInterruptedException("数据源oneid信息获取错误", "Your datasource's oneid configure occur error")
    }
    config
  }


}
