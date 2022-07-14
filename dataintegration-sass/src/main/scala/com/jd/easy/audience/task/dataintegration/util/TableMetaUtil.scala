package com.jd.easy.audience.task.dataintegration.util

import com.jd.easy.audience.common.constant.{NumberConstant, SplitDataSourceEnum, StringConstant}
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader}
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.sql.{Connection, ResultSet, Statement}
import java.text.{DecimalFormat, MessageFormat, SimpleDateFormat}
import java.util
import java.util.{Date, Locale, Map}
import scala.collection.JavaConversions.asScalaBuffer

object TableMetaUtil {
  private val LOGGER = LoggerFactory.getLogger(TableMetaUtil.getClass)
  private val BDP_DIR = "hadoop_conf"
  private val HADOOP_USER_NAME = "HADOOP_USER_NAME"
  private val HADOOP_PROXY_USER = "HADOOP_PROXY_USER"
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val CATATIME_FROMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH) //多态


  //  /**
  //   * 遍历文件，最近访问，最近修改和大小
  //   *
  //   * @param path
  //   * @param fileSystem
  //   */
  //  @Deprecated
  //  def listFileInfo(path: Path, fileSystem: FileSystem): TableMetaInfo = {
  //    try {
  //      val instatus = fileSystem.listStatus(path)
  //      var modificationTime: Long = 0L
  //      var fileSize: Long = 0L
  //      val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //      instatus.toList.foreach(fileEle => {
  //        if (fileEle.isDirectory) {
  //          LOGGER.info("is Directory:path=" + fileEle.getPath)
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-modificationTime=" + sd.format(fileEle.getModificationTime))
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-accessTime=" + sd.format(fileEle.getAccessTime))
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-len=" + fileEle.getLen)
  //          val directoryInfo: TableMetaInfo = listFileInfo(fileEle.getPath, fileSystem)
  //          modificationTime = Math.max(directoryInfo.getmodificationTime(), modificationTime)
  //          fileSize = fileSize + directoryInfo.getfileSize()
  //        } else {
  //          LOGGER.info("is File:path=" + fileEle.getPath)
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-modificationTime=" + sd.format(fileEle.getModificationTime))
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-accessTime=" + sd.format(fileEle.getAccessTime))
  //          LOGGER.info(fileEle.getPath + "=>listFileInfo-len=" + fileEle.getLen)
  //          modificationTime = Math.max(fileEle.getModificationTime, modificationTime)
  //          fileSize = fileSize + fileEle.getLen
  //        }
  //      })
  //      return new TableMetaInfo(modificationTime, fileSize, 0L, null)
  //    } catch {
  //      case e: Exception =>
  //        LOGGER.info("获取表信息有异常", e)
  //        return new TableMetaInfo(new Date().getTime, 0L, 0L, null)
  //    }
  //  }

  /**
   * 获取叶子目录，便于获取数据
   *
   * @param path
   * @param fileSystem
   * @return
   */
  def listLeafDir(path: Path, fileSystem: FileSystem): List[String] = {
    try {
      var dirList: List[String] = List()
      val instatus = fileSystem.listStatus(path)
      instatus.toList.foreach(fileEle => {
        if (fileEle.isDirectory) {
          LOGGER.info("is Directory:path=" + fileEle.getPath)
          val directoryInfo: List[String] = listLeafDir(fileEle.getPath, fileSystem)
          dirList = List.concat(dirList, directoryInfo)
        } else {
          LOGGER.info("is File:path=" + fileEle.getPath)
          val filePath: String = fileEle.getPath.toString
          dirList = dirList :+ filePath.substring(0, filePath.lastIndexOf("/"))
        }
      })
      return dirList.distinct
    } catch {
      case e: Exception =>
        LOGGER.info("获取表信息有异常", e)
        return List()
    }
  }

  def getTargetTableMetaInfo(dbName: String, targetTb: String, sparkSession: SparkSession): TableMetaInfo = {
    val startT: Long = new Date().getTime
    val tableLocation = getLocalUri(dbName, targetTb, sparkSession)
    val path = new Path(tableLocation)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val (modifiedTime, storageSize) = getModificationTime(path, fileSystem)
    val (rowNum, partitionColsList) = getCataNumRows(dbName, targetTb, sparkSession)
    val endT: Long = new Date().getTime
    LOGGER.info("getNumRows runtime:{}", (endT - startT).toDouble / NumberConstant.INT_1K)
    return new TableMetaInfo(modifiedTime, storageSize, rowNum, partitionColsList)
  }

  /**
   * @description 获取表的hdfs路径
   * @param [dbName, tableName, sparkSession]
   * @return java.lang.String
   * @date 2022/1/11 下午2:43
   * @auther cdxiongmei
   */
  def getLocalUri(dbName: String, tableName: String, sparkSession: SparkSession): String = {
    val descSql = s"DESC FORMATTED ${dbName}.${tableName}"
    LOGGER.info("descSql=" + descSql)
    val cataInfo = sparkSession.sql(descSql)
    var lacationurlNew = StringConstant.EMPTY
    var createTime = StringConstant.EMPTY
    var modifiedTime = StringConstant.EMPTY
    import scala.collection.JavaConversions._
    cataInfo.collectAsList().foreach(row => {
      if (ConfigProperties.FIELD_LOCATION.equalsIgnoreCase(row.getAs[String](ConfigProperties.FIELD_COL_NAME))) {
        lacationurlNew = row.getAs[String](ConfigProperties.FIELD_DATA_TYPE)
      } else if ("Table Properties".equalsIgnoreCase(row.getAs[String](ConfigProperties.FIELD_COL_NAME))) {
        var tablePro = row.getAs[String](ConfigProperties.FIELD_DATA_TYPE)
        tablePro.substring(NumberConstant.INT_1, tablePro.length - NumberConstant.INT_1).split(",").foreach(ele => {
          if (ele.split(StringConstant.EQUAL_CHAR)(NumberConstant.INT_0).equalsIgnoreCase("transient_lastDdlTime")) {
            var time = new Date(ele.split(StringConstant.EQUAL_CHAR)(NumberConstant.INT_1).toLong * NumberConstant.INT_1K)
            modifiedTime = SIMPLE_DATE_FORMAT.format(time)
          }
        })
      } else if ("Created Time".equalsIgnoreCase(row.getAs[String](ConfigProperties.FIELD_COL_NAME))) {
        createTime = SIMPLE_DATE_FORMAT.format(CATATIME_FROMAT.parse(row.getAs[String](ConfigProperties.FIELD_DATA_TYPE)))
      }
    })
    LOGGER.info("createTime= " + createTime + ", modifiedTime=" + modifiedTime)
    return lacationurlNew
  }

  /**
   * @description 获取最新更新时间
   * @param [path, fileSystem]
   * @return scala.Tuple2<java.lang.String,java.lang.Object>
   * @date 2022/1/11 下午2:11
   * @auther cdxiongmei
   */
  def getModificationTime(path: Path, fileSystem: FileSystem): (Long, Long) = {
    try {
      val instatus = fileSystem.listStatus(path)
      var modificationTime: Long = 0L
      var fileSize: Long = 0L
      //      val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      instatus.toList.foreach(fileEle => {
        if (fileEle.isDirectory) {
          var (timeTmp, sizeTmp) = getModificationTime(fileEle.getPath, fileSystem)
          fileSize = fileSize + sizeTmp
          modificationTime = Math.max(modificationTime, timeTmp)
        } else {
          fileSize = fileSize + fileEle.getLen
          modificationTime = Math.max(modificationTime, fileEle.getModificationTime)
        }
      })
      return (modificationTime, fileSize)
    } catch {
      case e: Exception =>
        LOGGER.info("获取表信息有异常", e)
        return (0L, 0L)
    }
  }

  def getDirectoryModificationTime(path: Path, fileSystem: FileSystem, bizTime: org.joda.time.DateTime, jobTime: org.joda.time.DateTime): java.util.Map[String, String] = {
    try {
      if (!fileSystem.exists(path)) {
        return new java.util.HashMap[String, String]()
      }
      val instatus = fileSystem.listStatus(path)
      val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var modificationTime: Long = 0L
      var modifiedInfos: java.util.Map[String, String] = new java.util.HashMap[String, String]()
      instatus.toList.foreach(fileEle => {
        println(fileEle.getPath)
        if (fileEle.isDirectory) {
          println("is Directory:path=" + fileEle.getPath + ", modifiedTime=" + sd.format(fileEle.getModificationTime))
          modifiedInfos.putAll(getDirectoryModificationTime(fileEle.getPath, fileSystem, bizTime, jobTime))
        } else {
          println("is File:path=" + fileEle.getPath + ", modifiedTime=" + sd.format(fileEle.getModificationTime))
          modificationTime = Math.max(fileEle.getModificationTime, modificationTime)
          if (jobTime.isAfter(modificationTime) && (bizTime.isBefore(modificationTime) || bizTime.isEqual(modificationTime))) {
            val filePath: String = fileEle.getPath.toString
            modifiedInfos.put(filePath.substring(0, filePath.lastIndexOf("/")), sd.format(modificationTime))
          }
        }
      })
      return modifiedInfos
    } catch {
      case e: Exception =>
        LOGGER.info("获取表信息有异常", e)
        return new java.util.HashMap[String, String]()
    }
  }

  /**
   * @description 获取表的数据记录数
   * @param [dbName, targetTable, sparkSession]
   * @return scala.Tuple2<java.lang.Object,java.util.List<java.lang.String>>
   * @date 2022/1/11 下午2:51
   * @auther cdxiongmei
   */
  def getCataNumRows(dbName: String, targetTable: String, sparkSession: SparkSession): (Long, java.util.List[String]) = {
    val partitionColsList: java.util.List[String] = new java.util.ArrayList[String]()
    sparkSession.catalog.listColumns(dbName, targetTable).filter("isPartition = 'true'").collectAsList().foreach(partitionCol => {
      partitionColsList.add(partitionCol.name)
    })
    if (CollectionUtils.isNotEmpty(partitionColsList)) {
      val partitionCnt = sparkSession.sql(s"SELECT COUNT(*) as cnt FROM $dbName.$targetTable").select("cnt").as(Encoders.LONG).collectAsList().get(0)
      return (partitionCnt, partitionColsList)
    }
    var analyzeSql: StringBuilder = new StringBuilder(s"""ANALYZE TABLE ${dbName}.${targetTable} """);
    analyzeSql.append("COMPUTE STATISTICS");
    LOGGER.info("元数据查询数据记录数:{}", analyzeSql.toString())
    var command: java.util.List[String] = new java.util.ArrayList[String]()
    command.add("hive")
    command.add("-e")
    command.add(analyzeSql.toString())
    val hiveProcessBuilder: ProcessBuilder = new ProcessBuilder(command);

    val hiveProcess: Process = hiveProcessBuilder.start();
    var bufferedReader = new BufferedReader(new InputStreamReader(hiveProcess.getErrorStream(), StandardCharsets.UTF_8))
    val pattern2 = ("""(.*) """ + dbName + """.""" + targetTable + """(\{.*\})? stats: \[(.*)\]$""").r
    LOGGER.info("stat pattern:{}", pattern2)
    var rowNumbers: Long = 0
    var fileSize: Long = 0
    //循环等待进程输出，判断进程存活则循环获取输出流数据
    while (hiveProcess.isAlive()) {
      while (bufferedReader.ready()) {
        val s: String = bufferedReader.readLine();
        //自定义进程输出处理
        s match {
          case pattern2(typePartition, partition, stat) => {
            stat.split(", ").foreach(f => {
              if (f.contains("numRows")) {
                rowNumbers += f.split("=")(1).toLong
              } else if (f.contains("totalSize")) {
                fileSize += f.split("=")(1).toLong
              }
            })
          }
          case _ =>
        }

      }
    }
    LOGGER.info("rowNumbers={}, fileSize={}", rowNumbers, fileSize)
    return (rowNumbers, partitionColsList)
  }
  def parsePartitionCond(filterExpress: String): java.util.Map[String, String] = {
    val condMap = new java.util.HashMap[String, String]
    if (StringUtils.isBlank(filterExpress)) return condMap
    val expressArr = filterExpress.toLowerCase.split("and")
    LOGGER.info("expressArr:{}", expressArr)
    for (expressItem <- expressArr) {
      val valueArr = expressItem.split("=")
      var fieldVal = StringUtils.trim(valueArr(1))
      if (fieldVal.startsWith("\"")) fieldVal = fieldVal.substring(1)
      if (fieldVal.endsWith("\"")) fieldVal = fieldVal.substring(0, fieldVal.length - 1)
      condMap.put(StringUtils.trim(valueArr(0)), fieldVal)
    }
    condMap
  }

  /**
   * 获取账号字段和库表之间关系（支持主子两种类型）
   *
   * @return
   */
  def getTenantRelInfo(sourceType: SplitSourceTypeEnum, dataSource: SplitDataSourceEnum, departId: Long, accountId: Long, accountName: String): LoadDataJnosRelInfo = {
    LOGGER.info("getTenantRelInfo dataSource=" + dataSource.getValue + ", departId=" + departId)
//    val expressCond = parsePartitionCond(fileExpress)
    var tenantRelInfo: LoadDataJnosRelInfo = null
    if (dataSource.getValue.equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue)) {
      val querySql =
        s"""
           |select
           |  account_id,
           |  main_account_name,
           |  crm_name,
           |  db_id,
           |  db_name,
           |  bdp_prod_account
           |from
           |  (
           |    select
           |      account_id,
           |      id as db_id,
           |      database_name as db_name,
           |      main_account_name,
           |      bdp_prod_account
           |    from
           |      ea_database
           |    where
           |      yn = 1
           |      and account_id = $accountId
           |  ) db_tb
           |  join (
           |    select
           |      crm_name,
           |      jnos_name
           |    from
           |      ea_crm_jnos_relation
           |    where
           |      yn = 1
           |  ) rel_tb on db_tb.main_account_name = rel_tb.jnos_name
           |""".stripMargin

      val db2 = DbManagerService.getConn //创建DBHelper对象
      val stmt = DbManagerService.stmt(db2)
      val retInfos = DbManagerService.executeQuery(stmt, querySql) //执行语句，得到结果集
      while ( {
        retInfos.next
      }) {
        //CRM中tenantcode为门店账号id，tenantname为jnos主账号名称
        tenantRelInfo = new LoadDataJnosRelInfo(retInfos.getLong("account_id"), retInfos.getString("main_account_name"), retInfos.getString("crm_name"), retInfos.getString("main_account_name"), -1L)
      }
      LOGGER.info("tenantRelInfo:" + tenantRelInfo.toString)
      DbManagerService.close(db2, stmt, null, retInfos)
    }
    else if (dataSource.getValue.equalsIgnoreCase(SplitDataSourceEnum.ACCOUNTID.getValue)) {
      tenantRelInfo = new LoadDataJnosRelInfo(accountId, accountName, accountId.toString, accountName, -1L)
    } else if (dataSource.getValue.equalsIgnoreCase(SplitDataSourceEnum.CA.getValue)) {
        tenantRelInfo = new LoadDataJnosRelInfo(accountId, accountName, accountId.toString, accountName, departId)
    }
    tenantRelInfo
  }

  /**
   * @description 新增表到mysql中
   * @param [sparkSession, beanCur, tableName]
   * @return void
   * @date 2022/1/11 下午4:32
   * @auther cdxiongmei
   */
  def writeTableToMysql(sparkSession: SparkSession, tabledesc: String, sourceType: SplitSourceTypeEnum, dataSource: SplitDataSourceEnum, departId: Long, accountId: Long, accountName: String, dbName: String, tableName: String): Unit = {
    //获取主子账号关系
    var classname = this.getClass.getSimpleName
    LOGGER.info("classname ={}", classname.substring(0, classname.length - 1))
    val templateConfig = ConfigProperties.templateInfo.getConfig(classname.substring(0, classname.length - 1)).getConfig(new Exception().getStackTrace()(0).getMethodName())
    val searchTemplate = templateConfig.getString("searchTable")
    val dbRelInfo = getTenantRelInfo(sourceType, dataSource, departId, accountId, accountName)
    LOGGER.info("dbRelInfo:" + dbRelInfo.toString)
    if (null == dbRelInfo) return
    //数据行数获取
    val fileInfo = TableMetaUtil.getTargetTableMetaInfo(dbName, tableName, sparkSession)
//    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val now = simpleDateFormat.format(new Date)
    val buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME)
    val jobTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val now = jobTime.toString("yyyy-MM-dd HH:mm:ss")
    //先查询是否已经存在记录
    var tableId = 0L
    val searchSql: String = MessageFormat.format(searchTemplate, dbName, tableName)
    val conn = DbManagerService.getConn
    val stmt = DbManagerService.stmt(conn)
    val ret = DbManagerService.executeQuery(stmt, searchSql)
    while ( {
      ret.next
    }) tableId = ret.getLong("id")
    if (tableId > BigInteger.ZERO.intValue) { //已经存在表，只需更新信息
      LOGGER.info("表信息已经存在，只需要更新tableId=" + tableId)
      val updateTemplate = templateConfig.getString("updateTable")
      val updateSql: String = MessageFormat.format(updateTemplate, fileInfo.getfileSizeFormat, fileInfo.getmodificationTimeFormat(), now, sourceType.getType.toString, fileInfo.getrowNumber().toString, StringUtils.join(fileInfo.getPartitions(), StringConstant.COMMA), dbRelInfo.getDepartmentId.toString, tableId.toString)
      val updateRow = DbManagerService.executeUpdate(stmt, updateSql)
      if (updateRow > BigInteger.ZERO.intValue) LOGGER.info("数据更新成功")
      DbManagerService.close(conn, stmt, null, null)
      return
    }
    //新增表信息
    LOGGER.info("表信息不存在，需要更新表和字段信息")
    var tableComment = StringConstant.EMPTY
    if (StringUtils.isNotBlank(tabledesc)) tableComment = tabledesc
    val insertTemplate = templateConfig.getString("insertTable")
    val insertSql = MessageFormat.format(insertTemplate,
      accountId.toString,
      accountName,
      dbRelInfo.getSubAccountName,
      dbName,
      tableName,
      tableComment,
      fileInfo.getfileSizeFormat(),
      fileInfo.getmodificationTimeFormat(),
      now,
      sourceType.getType.toString,
      fileInfo.getrowNumber().toString,
      StringUtils.join(fileInfo.getPartitions(), ","),
      dbRelInfo.getDepartmentId.toString
    )
    LOGGER.info("ea_create_table_info sql:" + insertSql)
    val updateRow = DbManagerService.executeUpdate(stmt, insertSql)
    if (updateRow <= BigInteger.ZERO.intValue) LOGGER.error("ea_create_table_info数据新增失败")
    //获取tableId
    val retId = DbManagerService.executeQuery(stmt, searchSql)
    while ( {
      retId.next
    }) tableId = retId.getLong(1)
    System.out.println("tableId:" + tableId)
    DbManagerService.close(conn, stmt, null, retId)
  }

  /**
   * 获取jnos账号和库的关系
   *
   * @return
   */
  def getJnosDatabaseInfo(): util.ArrayList[JnosRelInfo] = {
    var tenantRel = new util.ArrayList[JnosRelInfo]()
    val querySql: String =
      "    select\n" +
        "      account_id,\n" +
        "      main_account_name,\n" +
        "      id as db_id,\n" +
        "      database_name as db_name\n" +
        "    from\n" +
        "      ea_database\n" +
        "    where\n" +
        "      yn = 1;\n"
    val db2: Connection = DbManagerService.getConn(); //创建DBHelper对象
    val stmt: Statement = DbManagerService.stmt(db2)
    val retInfos: ResultSet = DbManagerService.executeQuery(stmt, querySql); //执行语句，得到结果集
    while (retInfos.next()) {
      LOGGER.info(s"accountId=${retInfos.getLong(1)},mainAccountName=${retInfos.getString(2)}, dbid=${retInfos.getLong(3)}, dbname=${retInfos.getString(4)}")
      //CRM中tenantcode为门店账号id，tenantname为jnos主账号名称
      tenantRel.add(new JnosRelInfo(retInfos.getLong(1), retInfos.getString(2), retInfos.getString(1), retInfos.getString(2), retInfos.getLong(3), retInfos.getString(4)))
    } //显示数据
    DbManagerService.close(db2, stmt, null, retInfos)
    return tenantRel
  }

  def getHadoopConf(bdpUser: String): Configuration = {
    val envParam = System.getenv
    val hadoopConfDir = envParam.getOrDefault("HADOOP_CONF_DIR", BDP_DIR)
    val conf = new Configuration
    if (BDP_DIR.equals(hadoopConfDir)) {
      conf.addResource(hadoopConfDir + "/core-site.xml")
      conf.addResource(hadoopConfDir + "/hdfs-site.xml")
    }
    else {
      conf.addResource(new Path(hadoopConfDir + "/core-site.xml"))
      conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"))
    }
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set(HADOOP_USER_NAME, bdpUser)
    conf.set(HADOOP_PROXY_USER, bdpUser)
    setEnv(HADOOP_USER_NAME, bdpUser)
    setEnv(HADOOP_PROXY_USER, bdpUser)
    conf
  }

  def setEnv(key: java.lang.String, value: java.lang.String): Unit = {
    try {
      System.setProperty(key, value)
      val env: Map[java.lang.String, java.lang.String] = System.getenv
      val cl = env.getClass
      val field = cl.getDeclaredField("m")
      field.setAccessible(true)
      val mapObject = field.get(env)
      if (mapObject.isInstanceOf[util.Map[_, _]]) {
        val writableEnv: Map[java.lang.String, java.lang.String] = mapObject.asInstanceOf[util.Map[java.lang.String, java.lang.String]]
        writableEnv.put(String.valueOf(key), value)
      }
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to set environment variable", e)
    }
  }
}

class TableMetaInfo(modificationTime: Long, fileSize: Long, rowNumber: Long, partitions: java.util.List[String]) {

  def getmodificationTime(): Long = {
    return modificationTime
  }

  def getmodificationTimeFormat(): String = {
    if (StringUtils.isBlank(modificationTime.toString) || modificationTime <= NumberConstant.INT_0) {
      return ""
    }
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(modificationTime)
    return newtime
  }

  def getfileSize(): Long = {
    return fileSize
  }

  def getrowNumber(): Long = {
    return rowNumber
  }

  def getfileSizeFormat(): String = {
    var sizeDouble: Double = fileSize.toDouble / 1024 / 1024
    return "".concat(new DecimalFormat("0.00").format(sizeDouble))
  }

  def getPartitions(): java.util.List[String] = {
    return partitions
  }

  override def toString: String = "{fileSize=" + fileSize + ",modificationTime=" + modificationTime + ", rowNumber=" + rowNumber + ", partitions=" + StringUtils.join(partitions, ",") + "}"
}
