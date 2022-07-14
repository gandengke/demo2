package com.jd.easy.audience.task.dataintegration.run.spark

import com.jd.easy.audience.task.dataintegration.util.DbManagerService

/**
 * ${description}
 *
 * @author cdxiongmei
 * @version V1.0
 */
object MysqlUpdate {
  def main(args: Array[String]): Unit = {
    val sql = args(0)
    println("sql=" + sql)
    val conn = DbManagerService.getConn
    val stmt = DbManagerService.stmt(conn)
    val ret: Int = DbManagerService.executeUpdate(stmt, sql)
    println("ret=" + ret)
  }

}
