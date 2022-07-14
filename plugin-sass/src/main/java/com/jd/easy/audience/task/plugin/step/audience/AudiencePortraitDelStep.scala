package com.jd.easy.audience.task.plugin.step.audience

import com.jd.easy.audience.task.commonbean.bean.AudiencePortraitDelBean
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import org.slf4j.LoggerFactory

import java.sql.{DriverManager, ResultSet, SQLException}
import java.util

/**
 * ck画像数据清理-ck表
 *
 * @author cdxiongmei
 * @date 2022/2/15 下午3:01
 * @version V1.0
 */
class AudiencePortraitDelStep extends StepCommon[Unit] {
  private val LOGGER = LoggerFactory.getLogger(classOf[AudiencePortraitDelStep])

  override def run(dependencies: util.Map[String, StepCommonBean[_]]) = {

    // 1.环境准备
    val bean: AudiencePortraitDelBean = getStepBean.asInstanceOf[AudiencePortraitDelBean]
    val portraitId = bean.getPortraitId
    deleteClickHouseTable(portraitId)
  }

  /**
   * @description 功能描述: 删除ck表
   * @author cdxiongmei
   * @date 2022/4/22 3:53 PM
   * @param
   * @return
   */
  @throws[ClassNotFoundException]
  @throws[SQLException]
  private def deleteClickHouseTable(portraitId: String): Unit = {
    LOGGER.info("ready to delete ckTable")
    val jdbcUrl = s"""jdbc:clickhouse://${ConfigProperties.CK_URL}"""
    val databaseName = ConfigProperties.CK_DBNAME
    val user = ConfigProperties.CK_USER
    val password = ConfigProperties.CK_PASS
    val tableName = ConfigProperties.CK_TABLEPREFIX + "%_" + portraitId + ConfigProperties.CK_TABLESUFFIX
    val ckDriver = "ru.yandex.clickhouse.ClickHouseDriver"
    Class.forName(ckDriver) //指定连接类型
    val conn = DriverManager.getConnection(jdbcUrl, user, password)
    val statement = conn.createStatement
    val tableLikeSql = s"""
                          |SELECT
                          |	database,
                          |	name
                          |FROM
                          |	system.tables
                          |WHERE
                          |	(
                          |		database = '${databaseName}'
                          |	)
                          |	AND
                          |	(
                          |		name LIKE '${tableName}'
                          |	);
                          |""".stripMargin
    LOGGER.info("tableLikeSql:" + tableLikeSql)
    val ret: ResultSet = statement.executeQuery(tableLikeSql)
    LOGGER.info("tablename like :" + tableName)
    while (ret.next()) {
      val table = ret.getString("name")
      LOGGER.info("delete table:" + table)
//      val truncateSql = "TRUNCATE TABLE " + databaseName + "." + table + "_local on cluster default;"
//      System.out.println("truncatesql:" + truncateSql)
//      statement.execute(truncateSql)
      //删除表
      val distDelSql = "DROP TABLE IF EXISTS " + databaseName + "." + table + " on cluster default;"
      System.out.println("deletesql:" + distDelSql)
      statement.execute(distDelSql)
      val localDelSql = "DROP TABLE IF EXISTS " + databaseName + "." + table + "_local on cluster default;"
      System.out.println("deletesql_local:" + localDelSql)
      statement.execute(localDelSql)
    }
    ret.close()
    statement.close()
    conn.close()
  }

}
