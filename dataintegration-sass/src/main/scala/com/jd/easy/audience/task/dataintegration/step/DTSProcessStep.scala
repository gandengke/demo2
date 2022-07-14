package com.jd.easy.audience.task.dataintegration.step

import java.util

import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import com.jd.easy.audience.task.dataintegration.util.DataFrameOps._
import com.jd.easy.audience.task.dataintegration.util.SparkUtil
import com.jd.easy.audience.task.driven.step.{StepCommon, StepCommonBean}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Encoders
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

@Deprecated
class DTSProcessStep extends StepCommon[Unit]{

  val log: Logger = LoggerFactory.getLogger(classOf[DTSProcessStep])
  val databaseTableName = "@tableName@"
  val BUFFALO_ENV_BCYCLE = "BUFFALO_ENV_BCYCLE"

  def run(dependencies: util.Map[String, StepCommonBean[_]]): Unit = {
    log.info(s"env: ${System.getenv()}")
    val buffaloCycle = System.getenv(BUFFALO_ENV_BCYCLE)
    log.info(s"BUFFALO_ENV_BCYCLE: $buffaloCycle")
    val path = ConfigProperties.DTS_HDFS_PATH
    if (StringUtils.isBlank(buffaloCycle)) {
      throw new JobInterruptedException("argument is invalid")
    }
    val dateTime = DateTime.parse(buffaloCycle, DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss"))
    val lastPeriod = dateTime.minusMinutes(15)
    val directory = lastPeriod.toString("yyyy-MM-dd/HH/") + lastPeriod.getMinuteOfHour / 15
    val targetPath = s"$path/$directory/"
    log.info(s"target path: $targetPath")


    val spark = SparkUtil.buildMergeFileSession("DTSProcessStep")

    if (!isExsit(targetPath, spark.sparkContext.hadoopConfiguration)) {
      log.info(s"Path does not exist: $targetPath")
      return
    }
    val df = spark.read.json(targetPath)
    df.cache()
    df.printSchema()
    val count = df.count()
    if (count == 0) return

    val tableArray: Array[String] = df.select(databaseTableName).as(Encoders.STRING).distinct().collect()
    val existTableArray = tableArray.filter(tableName => {
      if (!spark.catalog.tableExists(tableName)) {
        log.error(s"Error: the table $tableName is not exist!")
        false
      } else {
        true
      }
    })
    log.info(s"insert data to tables: " + existTableArray.mkString(","))
    if (existTableArray.length == 0) throw new RuntimeException("no exist table!")

    val exceptionList = new ListBuffer[Exception]
    val successTable = new ListBuffer[String]
    existTableArray.foreach(tableName =>
      try {
        df.filter(s"$databaseTableName = '$tableName'").saveToHive(tableName, true)
        successTable.append(tableName)
      } catch {
        case e: Exception =>
          log.error(s"save data error: $tableName", e)
          exceptionList.append(e)
      }
    )

    log.info("success table name: " + successTable.mkString(", "))

    if (exceptionList.nonEmpty) {
      sys.exit(-1)
      throw new RuntimeException("Exception exist!")
    }
  }
  private def isExsit(desPath: String, conf: org.apache.hadoop.conf.Configuration): Boolean = {
    val fileSystem = FileSystem.get(conf)
    fileSystem.exists(new Path(desPath))
  }
}
