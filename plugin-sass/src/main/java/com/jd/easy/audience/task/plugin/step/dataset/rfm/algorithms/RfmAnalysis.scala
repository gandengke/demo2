package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms

import java.util
import java.util.HashMap

import com.google.common.collect.{Range, TreeRangeMap}
import com.jd.easy.audience.common.oss.{JdCloudDataConfig, OssFileTypeEnum}
import com.jd.easy.audience.common.util.JdCloudOssUtil
import com.jd.easy.audience.task.plugin.property.ConfigProperties
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.result.{IndicatorBucketResult, IndicatorGeneralResult, LevelCrowdResult, RfmAnalysisTaskResult}
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo.{RfmAnalysisConfigInfo, RfmAnalysisTaskNew}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Iterator, mutable}
import scala.reflect.io.File

object RfmAnalysis {

  val logger: Logger = LoggerFactory.getLogger(RfmAnalysis.getClass)

  def RenameAndDeleteHdfsFile(spark: SparkSession,
                              detailTempPath: String,
                              detailHdfsPath: String): Unit = {
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tempPath = new Path(detailTempPath)
    val hdfsPath = new Path(detailHdfsPath)
    val successPath = new Path(detailTempPath + "/_SUCCESS")
    if (fileSystem.exists(successPath)) {
      fileSystem.delete(successPath, true)
    }
    val parquetPath = fileSystem.listStatus(tempPath)(0).getPath
    fileSystem.rename(parquetPath, hdfsPath)
    fileSystem.delete(tempPath, true)
  }

  def getAvgMedian(spark: SparkSession,
                   indicatorDF: DataFrame,
                   splitsDF: DataFrame,
                   cycleCount: Map[Long, Long]):
  (Map[(Long, String), Double], Map[(Long, String), Double]) = {
    import spark.implicits._
    // 均值: 最近购买时长R 购买频次F 复购周期C 消费金额M
    val meanRFMC: Map[(Long, String), Double] = indicatorDF.selectExpr("task_id", "recency", "frequency",
      "if(monetary<0,0.0,monetary) as monetary_for_mm",
      "cycle", "amount")
      .groupBy("task_id")
      .agg(avg("recency").as("mean_recency"),
        avg("frequency").as("mean_frequency"),
        avg("cycle").as("mean_cycle"),
        avg("monetary_for_mm").as("mean_monetary"),
        avg("amount").as("mean_amount"))
      .na.fill(-1, Seq("mean_cycle"))
      .as[(Long, Double, Double, Double, Double, Double)]
      .flatMap { case (id, meanR, meanF, meanC, meanM, meanA) =>
        val res = ArrayBuffer[((Long, String), Double)]()
        res.append(((id, "recency"), meanR.formatted("%.2f").toDouble))
        res.append(((id, "frequency"), meanF.formatted("%.2f").toDouble))
        res.append(((id, "monetary"), meanM.formatted("%.2f").toDouble))
        if (cycleCount.contains(id)) res.append(((id, "cycle"), meanC.formatted("%.2f").toDouble))
        res.append(((id, "amount"), meanA.formatted("%.2f").toDouble))
        res.iterator
      }.collect().toMap
    logger.info(s"rfm-log: meanRFMC $meanRFMC")

    // 中位数: 最近购买时长R 购买频次F 复购周期C 消费金额M
    val medianRFMC: Map[(Long, String), Double] = splitsDF
      .as[(Long, Array[Double], Array[Double], Double, Array[Double], Array[Double], Array[Double])]
      .flatMap { case (id, splitR, splitF, medianM, _, splitC, splitA) =>
        val res = ArrayBuffer[((Long, String), Double)]()
        res.append(((id, "recency"), splitR(splitR.length - 1).formatted("%.2f").toDouble))
        res.append(((id, "frequency"), splitF(splitF.length - 1).formatted("%.2f").toDouble))
        res.append(((id, "monetary"), medianM.formatted("%.2f").toDouble))
        if (cycleCount.contains(id)) res.append(((id, "cycle"), splitC(splitC.length - 1).formatted("%.2f").toDouble))
        res.append(((id, "amount"), splitA(splitA.length - 1).formatted("%.2f").toDouble))
        res.iterator
      }.collect().toMap
    logger.info(s"rfm-log: medianRFMC $medianRFMC")

    (meanRFMC, medianRFMC)
  }

  def getSplits(indicator: String,
                minborder: Double,
                maxBorder: Double):
  Array[Range[java.lang.Double]] = {
    val splitIntervals = ArrayBuffer[Range[java.lang.Double]]()
    val minBorder = math.round(minborder).toDouble
    indicator match {
      case "monetary" | "Price" =>
        val interval = if (math.floor((maxBorder - minBorder) / 6.0) < 1) 1 else math.floor((maxBorder - minBorder) / 6.0)
        splitIntervals.append(Range.open[java.lang.Double](Double.NegativeInfinity, minBorder + interval))
        for (i <- 1 to 4)
          splitIntervals.append(Range.closedOpen[java.lang.Double](minBorder + i * interval, minBorder + (i + 1) * interval))
        splitIntervals.append(Range.closedOpen[java.lang.Double](minBorder + 5 * interval, Double.PositiveInfinity))
      case "frequency" | "amount" =>
        val interval = if (math.floor((maxBorder - (minBorder + 3)) / 3.0) < 1) 1 else math.floor((maxBorder - (minBorder + 3)) / 3.0)
        for (i <- 0 to 2)
          splitIntervals.append(Range.closed[java.lang.Double](minBorder + i, minBorder + i))
        for (i <- 0 to 1)
          splitIntervals.append(Range.closed[java.lang.Double](minBorder + 3 + i * interval, minBorder + 3 + (i + 1) * interval - 1))
        splitIntervals.append(Range.closedOpen[java.lang.Double](minBorder + 3 + 2 * interval, Double.PositiveInfinity))
      case "recency" | "cycle" =>
        val interval = if (math.floor((maxBorder - minBorder) / 6.0) < 1) 1 else math.floor((maxBorder - minBorder) / 6.0)
        for (i <- 0 to 4)
          if (indicator == "recency") {
            splitIntervals.append(Range.closed[java.lang.Double](minBorder + i * interval, minBorder + (i + 1) * interval - 1))
          }
          else {
            splitIntervals.append(Range.closedOpen[java.lang.Double](minBorder + i * interval, minBorder + (i + 1) * interval))
          }
        splitIntervals.append(Range.closedOpen[java.lang.Double](minBorder + 5 * interval, Double.PositiveInfinity))
    }
    splitIntervals.toArray
  }

  def getParametersForBucket(spark: SparkSession,
                             splitsWithID: Map[(Long, String), Array[Range[java.lang.Double]]]):
  (Array[(Long, String, Int, String, Double, Double)], UserDefinedFunction) = {
    /**
     * bucketAll
     * 01      task_id             Long        任务ID
     * 02      indicator           String      指标名称
     * 03      rank                Int         分箱区间序号
     * 04      bucket              String      分箱区间
     * 05      upBorder            Double      分箱区间上界值
     * 06      lowBorder           Double      分箱区间下界值
     */
    val bucketAll: Array[(Long, String, Int, String, Double, Double)] = splitsWithID.toArray
      .flatMap { case ((id, indicator), splits: Array[Range[java.lang.Double]]) =>

        if (indicator == "channel") {
          Iterator.range(1, 6).map { i =>
            (id, indicator, i, i.toString, 0.0, 0.0)
          }
        } else {
          splits.iterator.map { range: Range[java.lang.Double] =>
            val index = splits.indexOf(range)
            var interval = range.toString.replace("‥", ",")
            var upBorder = range.upperEndpoint().toDouble
            var lowBorder = range.lowerEndpoint().toDouble
            if (index == 0 && (indicator == "monetary" || indicator == "price")) {
              interval = "<" + upBorder.toInt
              lowBorder = -99999999.0
            }
            if (index == splits.length - 1) {
              interval = ">=" + lowBorder.toInt
              upBorder = 99999999.0
            }
            (id, indicator, index, interval, upBorder, lowBorder)
          }
        }
      }
    logger.info("rfm-log: bucketAll")

    // 分桶udf
    val bucketUdf = udf { (taskID: Long, value: Double, indicator: String) =>
      if (indicator == "cycle" && value < 0) ""
      else {
        val splits = splitsWithID(taskID, indicator)
        val rangeMap = TreeRangeMap.create[java.lang.Double, Int]()
        splits.iterator.foreach { range =>
          rangeMap.put(range, splits.indexOf(range))
        }
        val index = rangeMap.get(value)
        val range = splits(index)
        if (index == 0 && indicator == "monetary")
          "<" + range.upperEndpoint().toInt
        else if (index == splits.length - 1)
          ">=" + range.lowerEndpoint().toInt
        else
          range.toString.replace("‥", ",")
      }
    }

    (bucketAll, bucketUdf)
  }

  def getParametersForLevel(splitsWithID: Map[(Long, String), Array[Range[java.lang.Double]]],
                            meanRFMC: Map[(Long, String), Double],
                            medianRFMC: Map[(Long, String), Double],
                            orderCountWithID: Map[Long, Long]):
  (Map[(Long, String), Array[Double]], UserDefinedFunction, UserDefinedFunction, UserDefinedFunction) = {

    val baselineWithID: Map[(Long, String), Array[Double]] = splitsWithID
      .filter { case ((_, indicator), _) => indicator == "recency" | indicator == "frequency" | indicator == "monetary" }
      .map { case ((id, indicator), splitIntervals: Array[Range[java.lang.Double]]) =>
        val mean = meanRFMC(id, indicator)
        val median = medianRFMC(id, indicator)
        val splits = ArrayBuffer[Double]()
        Iterator.range(1, splitIntervals.length).foreach { i =>
          splits.append(splitIntervals(i).lowerEndpoint())
        }
        val points = (splits.toArray ++ Array(mean, median)).distinct.sorted
        ((id, indicator), points)
      }
    logger.info(s"rfm-log: baselineWithID")

    val levelBucketUdf = udf { (taskID: Long, value: Double, indicator: String) =>
      val splits = (baselineWithID(taskID, indicator) ++ Array(Double.NegativeInfinity, Double.PositiveInfinity)).sorted
      val rangeMap = TreeRangeMap.create[java.lang.Double, Int]()
      Iterator.range(0, splits.length - 1).map { i =>
        (i, i + 1)
      }.foreach { case (i, j) =>
        rangeMap.put(Range.openClosed[java.lang.Double](splits(i), splits(j)), i)
      }
      rangeMap.get(value)
    }

    val levelPercentUdf = udf { (id: Long, cnt: Long) => cnt.toDouble / orderCountWithID(id).toDouble }

    val levelRankUdf = udf { level_cd: String =>
      level_cd match {
        case "011" => 1
        case "111" => 2
        case "001" => 3
        case "101" => 4
        case "010" => 5
        case "000" => 6
        case "110" => 7
        case "100" => 8
      }
    }
    (baselineWithID, levelBucketUdf, levelPercentUdf, levelRankUdf)
  }

  def computeImpl(spark: SparkSession,
                  jfsBucket: String,
                  taskGroup: Array[RfmAnalysisTaskNew]
                 ): (Array[RfmAnalysisTaskResult], Map[(Long, String), Double], Map[(Long, String), Double]) = {
    import spark.implicits._
    implicit val myEncoder: Encoder[((Long, String), Array[Range[java.lang.Double]])] = Encoders.kryo[((Long, String), Array[Range[java.lang.Double]])]
    val tempDir = "/tmp/RfmAnalysis_EA/" + spark.sparkContext.applicationId + "_" + System.currentTimeMillis

    logger.info(s"rfm-log: tempDir $tempDir")
    val conf = spark.sparkContext.hadoopConfiguration
    //    FileSystem.setDefaultUri(conf, "hdfs://ns1016")
    val ossBucket = (if (jfsBucket.startsWith("/")) jfsBucket.substring(1) else jfsBucket).stripSuffix("/")

    //    val taskFile = "tasksInfo.parquet"
    //    logger.info(s"rfm-log: taskArr Path: $tempDir/$taskFile")
    //    taskGroup.toSeq.toDF().repartition(1).write.mode(SaveMode.Overwrite).parquet(s"$tempDir/$taskFile")
    logger.info("rfm-log: taskArr written successfully")

    val endDateMap: Map[Long, String] = taskGroup.map(task => (task.taskId, task.endDate)).toMap
    val endDateUdf = udf { task_id: Long => endDateMap(task_id) }

    val trueFlag = true
    val schema = StructType(List(
      StructField("task_id", LongType, trueFlag),
      StructField("user_log_acct", StringType, trueFlag),
      StructField("recency", IntegerType, trueFlag),
      StructField("frequency", LongType, trueFlag),
      StructField("monetary", DoubleType, trueFlag),
      StructField("cycle", DoubleType, trueFlag),
      StructField("amount", IntegerType, trueFlag),
      StructField("channel", ArrayType(StringType, trueFlag), trueFlag)
    ))
    var indicatorDFPre = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    /** 交易数据
     * user_log_acct       string	用户名
     * parent_sale_ord_id  bigint	父订单ID
     * monetary            double	消费金额
     * amount              int	    购买数量
     * channel             string	购买渠道
     * dt                  string	订单日期
     * task_id             bigint
     */
    var dataSizeWithID = mutable.Map.empty[Long, Long]
    var validDataSizeWithID = mutable.Map.empty[Long, Long]
    val orderTask = taskGroup.filter(_.dataType == 1).map(_.taskId)
    var orderData = spark.emptyDataset[(String, String, Double, Int, String, String, Long)]
    if (orderTask.nonEmpty) {
      taskGroup.filter(_.dataType == 1).foreach { task =>
        val data = task.dataSet.withColumn("task_id", lit(task.taskId)).as[(String, String, Double, Int, String, String, Long)];
        logger.info("orderdata:")
        orderData = orderData.union(data)
      }
      val dataDF = orderData.toDF("user_log_acct", "parent_sale_ord_id", "monetary", "amount", "channel", "dt", "task_id")
      dataSizeWithID ++= dataDF.groupBy("task_id").count().as[(Long, Long)].collect().toMap
      val validStr = dataDF.columns.map(_ + " is not null").mkString(" and ")
      val validDataDF = dataDF.filter(validStr)
      validDataSizeWithID ++= validDataDF.groupBy("task_id").count().as[(Long, Long)].collect().toMap
      val orderDataDF = validDataDF
        .groupBy("task_id", "user_log_acct")
        .agg(max("dt").as("latest_dt"),
          min("dt").as("first_dt"),
          countDistinct("parent_sale_ord_id").as("frequency"),
          sum("monetary").as("monetary"),
          sum("amount").as("amount"),
          collect_set(col("channel")).as("channel"))
        .withColumn("end_date", endDateUdf(col("task_id")))
        .withColumn("recency", datediff(col("end_date"), col("latest_dt")))
        .withColumn("cycle", datediff(col("latest_dt"), col("first_dt")) / (col("frequency") - 1))
        .select("task_id", "user_log_acct", "recency", "frequency", "monetary", "cycle", "amount", "channel")
      indicatorDFPre = indicatorDFPre.union(orderDataDF)
    }

    /** 客户数据
     * user_log_acct       string          用户名
     * frequency           int             累计消费次数
     * monetary            double          累计消费金额
     * amount              int             累计购买数量
     * channel             Array[string]   购买渠道
     * first_dt            string          首次消费日期
     * latest_dt           string          最近一次消费日期
     * task_id             bigint
     */
    val userTask = taskGroup.filter(_.dataType == 2).map(_.taskId)
    var userData = spark.emptyDataset[(String, Int, Double, Int, Array[String], String, String, Long)]
    if (userTask.nonEmpty) {
      taskGroup.filter(_.dataType == 2).foreach { task =>
        val data = task.dataSet
          .withColumn("task_id", lit(task.taskId))
          .as[(String, Int, Double, Int, Array[String], String, String, Long)]
        logger.info("userdata:")
        userData = userData.union(data)
      }
      val dataDF = userData.toDF("user_log_acct", "frequency", "monetary", "amount", "channel", "first_dt", "latest_dt", "task_id")
      dataSizeWithID ++= dataDF.groupBy("task_id").count().as[(Long, Long)].collect().toMap
      val validStr = dataDF.columns.map(_ + " is not null").mkString(" and ")
      val validDataDF = dataDF.filter(validStr)
      validDataSizeWithID ++= validDataDF.groupBy("task_id").count().as[(Long, Long)].collect().toMap

      val userDataDF = validDataDF.filter("frequency>0")
        .withColumn("end_date", endDateUdf(col("task_id")))
        .withColumn("recency", datediff(col("end_date"), col("latest_dt")))
        .withColumn("cycle", datediff(col("latest_dt"), col("first_dt")) / (col("frequency") - 1))
        .select("task_id", "user_log_acct", "recency", "frequency", "monetary", "cycle", "amount", "channel")
      indicatorDFPre = indicatorDFPre.union(userDataDF)
    }
    logger.info(s"rfm-log: dataSizeWithID $dataSizeWithID")
    logger.info(s"rfm-log: validDataSizeWithID $validDataSizeWithID")

    /**
     * indicatorDFCopy
     * 01      task_id             Long               任务ID
     * 02      user_log_acct       String             用户名
     * 03      recency             Int                最近购买时长
     * 04      frequency           Long               购买频次
     * 05      monetary            Double             消费金额
     * 06      cycle               Double             复购周期
     * 07      amount              Long               购买件数
     * 08      channel             Array[String]      下单渠道
     */

    val indicatorFile = "indicatorDF.parquet"
    logger.info(s"rfm-log: indicatorDF Path: $tempDir/$indicatorFile")
    indicatorDFPre.repartition(taskGroup.length * 20).write.mode(SaveMode.Overwrite).parquet(s"$tempDir/$indicatorFile")
    logger.info("rfm-log: indicatorDF written successfully")
    val indicatorDF = spark.read.parquet(s"$tempDir/$indicatorFile")

    //用户明细保存到oss
    taskGroup.foreach { task =>
      val taskDF = indicatorDF.filter(s"task_id='${task.taskId}'").drop("task_id")
      val detailOssPath = JdCloudOssUtil.getRfmUserDetDataKey(task.outputPath)

      val config: JdCloudDataConfig = new JdCloudDataConfig(task.endPoint, task.accessKey, task.secretKey)
      config.setOptions(new util.HashMap[String, String]() {{put("header", "true")}})
      config.setClientType(ConfigProperties.getClientType)
      config.setFileType(OssFileTypeEnum.parquet)
      config.setData(taskDF)
      config.setObjectKey(ossBucket + File.separator + detailOssPath)
      val packageUrl: String = JdCloudOssUtil.writeDatasetToOss(spark, config)
      logger.info(s"rfm-log: oss $packageUrl written successfully")
    }

    // 购买人数
    val orderCountWithID = indicatorDF
      .select("task_id", "user_log_acct")
      .groupBy("task_id")
      .agg(count("user_log_acct").as("cnt"))
      .as[(Long, Long)]
      .collect().toMap
    logger.info(s"rfm-log: orderCountWithID $orderCountWithID")

    // 购买占比
    val orderPercentWithID = orderCountWithID.map { case (id, size) =>
      (id, size.toDouble / validDataSizeWithID(id))
    }
    logger.info(s"rfm-log: orderPercentWithID $orderPercentWithID")

    val noOrderTaskGroup = taskGroup.filter(task => !orderCountWithID.keySet.contains(task.taskId))
    println("noOrderTaskGroup show" + noOrderTaskGroup.toList.toString())

    val orderTaskGroup = taskGroup.filter(task => orderCountWithID.keySet.contains(task.taskId))

    val cycleCount: Map[Long, Long] = indicatorDF.select("task_id", "user_log_acct", "cycle")
      .filter("Cycle is not null")
      .groupBy("task_id")
      .agg(count("user_log_acct").as("cnt"))
      .select("task_id", "cnt")
      .as[(Long, Long)]
      .collect().toMap
    logger.info(s"rfm-log: cycleCount $cycleCount")

    // 复购率
    val reOrderPercentWithID: Map[Long, Double] = cycleCount.map { case (id, cycleCnt) =>
      (id, cycleCnt.toDouble / orderCountWithID(id).toDouble)
    }
    logger.info(s"rfm-log: reOrderPercentWithID $reOrderPercentWithID")

    //用户数量-》去除最大5%数据
    //分位点函数
    indicatorDF.selectExpr("task_id", "user_log_acct", "recency", "frequency",
      "if(monetary<0,0.0,monetary) as monetary_for_mm",
      "if(monetary<0,null,monetary) as monetary_for_split",
      "cycle", "amount")
      .createOrReplaceTempView("indicator")

    val splitsDF = spark.sql(
      s"""
         |select task_id,
         |       percentile_approx(recency, Array(0,0.95,0.5)) as recency,
         |       percentile_approx(frequency, Array(0,0.95,0.5)) as frequency,
         |       percentile_approx(monetary_for_mm, 0.5) as monetary_for_mm,
         |       percentile_approx(monetary_for_split, Array(0,0.95)) as monetary_for_split,
         |       percentile_approx(cycle, Array(0,0.95,0.5)) as cycle,
         |       percentile_approx(amount, Array(0,0.95,0.5)) as amount
         |from indicator
         |group by task_id
         """.stripMargin)

    val (meanRFMC, medianRFMC) = getAvgMedian(spark, indicatorDF, splitsDF, cycleCount)

    val splitsWithID = splitsDF.as[(Long, Array[Double], Array[Double], Double, Array[Double], Array[Double], Array[Double])]
      .flatMap { case (taskId, splitR, splitF, _, splitM, splitC, splitA) =>
        val res = ArrayBuffer[((Long, String), Array[Range[java.lang.Double]])]()
        res.append(((taskId, "recency"), getSplits("recency", splitR(0), splitR(1))))
        res.append(((taskId, "frequency"), getSplits("frequency", splitF(0), splitF(1))))
        res.append(((taskId, "monetary"), getSplits("monetary", splitM(0), splitM(1))))
        if (cycleCount.contains(taskId)) res.append(((taskId, "cycle"), getSplits("cycle", splitC(0), splitC(1))))
        res.append(((taskId, "amount"), getSplits("amount", splitA(0), splitA(1))))
        res.append(((taskId, "channel"), Array.empty[Range[java.lang.Double]]))
        res.iterator
      }.collect().toMap

    val (bucketAll, bucketUdf) = getParametersForBucket(spark, splitsWithID)

    /**
     * bucketedWithID
     * 01      task_id                      Long               任务ID
     * 02      user_log_acct                String             用户名
     * 03      channel                      Array[String]      下单渠道
     * 04      bucketed_recency             String             最近购买时长分箱
     * 05      bucketed_monetary            String             消费金额分箱
     * 06      bucketed_cycle               String             复购周期分箱
     * 07      bucketed_frequency           String             购买频次分箱
     * 08      bucketed_amount              String             购买件数分箱
     */
    var bucketedWithID = indicatorDF.na.fill(-1.0)
    Array("recency", "monetary", "cycle", "frequency", "amount").foreach { indicator =>
      bucketedWithID = bucketedWithID
        .withColumn(s"bucketed_$indicator", bucketUdf(col("task_id"), col(indicator), lit(indicator)))
        .drop(indicator)
    }
    logger.info(s"rfm-log: bucketedWithID")

    // 分桶结果聚合 Recency,Frequency,Monetary,Cycle,Amount,Channel
    // Channel 下单渠道
    /**
     * groupBucketDF
     * 01      task_id             Long        任务ID
     * 02      indicator           String      指标名称
     * 03      bucket              String      分箱结果
     * 04      cnt                 Long        该分箱人数
     */
    val groupBucketDF = bucketedWithID
      .select("task_id", "user_log_acct", "bucketed_recency", "bucketed_frequency", "bucketed_monetary", "bucketed_cycle", "bucketed_amount", "channel")
      .as[(Long, String, String, String, String, String, String, Array[String])]
      .flatMap { case (id, user, bucketedRecency, bucketedFrequency, bucketedMonetary, bucketedCycle, bucketedAmount, channelArr) =>
        val res = mutable.ArrayBuffer.empty[(Long, String, String, String)]
        res.append((id, "recency", bucketedRecency, user))
        res.append((id, "frequency", bucketedFrequency, user))
        res.append((id, "monetary", bucketedMonetary, user))
        res.append((id, "cycle", bucketedCycle, user))
        res.append((id, "amount", bucketedAmount, user))
        channelArr.iterator.map((id, "channel", _, user)) ++ res.iterator
      }.toDF("task_id", "indicator", "bucket", "user_log_acct")
      .groupBy("task_id", "indicator", "bucket")
      .agg(count("user_log_acct").as("cnt"))
      .filter("bucket!=''")
    // groupBucketDF save to hdfs
    val groupBucketFile = "groupBucketDF.parquet"
    logger.info(s"rfm-log: groupBucket path: $tempDir/$groupBucketFile")
    groupBucketDF.repartition(1).write.mode(SaveMode.Overwrite).parquet(s"$tempDir/$groupBucketFile")
    logger.info(s"rfm-log: groupBucket written successfully")

    val groupBucketMap = groupBucketDF.as[(Long, String, String, Long)]
      .map { case (taskId, indicator, bucket, cnt) => ((taskId, indicator, bucket), cnt) }
      .collect().toMap
    logger.info(s"rfm-log: groupBucked")

    /**
     * groupBucketAllDF
     * 01      task_id             Long        任务ID
     * 02      indicator           String      指标名称
     * 03      bucket              String      分箱结果
     * 04      rank                Int         分箱区间序号
     * 05      upBorder            Double      分箱区间上限值
     * 06      lowBorder           Double      分箱区间下限值
     * 07      cnt                 Long        该分箱人数
     * 08      percent             Double      该分箱人数占比
     */

    //bucket result
    val groupBucketAll = bucketAll
      .map { case (taskId, indicator, rank, bucket, upBorder, lowBorder) =>
        val cnt = groupBucketMap.getOrElse((taskId, indicator, bucket), 0L)
        val size = if (indicator == "cycle") cycleCount.getOrElse(taskId, 0L) else orderCountWithID(taskId)
        val percent = if (size == 0) 0.0 else cnt.toDouble / size.toDouble
        val bucketRes = if (upBorder == lowBorder && indicator != "channel") upBorder.toInt.toString else bucket

        ((taskId, indicator), IndicatorBucketResult(rank, bucketRes, cnt, upBorder, lowBorder, percent))
      }

    val groupBucketAllMap = spark.sparkContext.parallelize(groupBucketAll)
      .groupByKey().collect()
      .map { case ((id, indicator), buckets) => ((id, indicator), buckets.toArray) }.toMap
    logger.info(s"rfm-log: groupBucketAllMap")

    val (baselineWithID, levelBucketUdf, levelPercentUdf, levelRankUdf) = getParametersForLevel(splitsWithID, meanRFMC, medianRFMC, orderCountWithID)
    // 用户分层
    /**
     * pinLevel
     * 01      task_id             Long        任务ID
     * 02      recency             Double      最近购买时长
     * 03      frequency           Double      购买频次
     * 04      monetary            Double      消费金额
     * 05      level_rank          Int         人群分层序号
     * 06      level_cd            String      人群分层描述
     * 07      level_cnt           Long        人数
     * 08      level_percent       Double      占比
     */
    val pinLevelDF = indicatorDF.select("task_id", "user_log_acct", "recency", "frequency", "monetary")
      .withColumn("level_recency", levelBucketUdf(col("task_id"), col("recency"), lit("recency")))
      .withColumn("level_frequency", levelBucketUdf(col("task_id"), col("frequency"), lit("frequency")))
      .withColumn("level_monetary", levelBucketUdf(col("task_id"), col("monetary"), lit("monetary")))
      .groupBy("task_id", "level_recency", "level_frequency", "level_monetary")
      .agg(count("user_log_acct").as("level_cnt"))
      .as[(Long, Int, Int, Int, Long)]
      .mapPartitions { iter =>
        iter.flatMap { case (id, levelRecency, levelFrequency, levelMonetary, levelCnt) =>
          baselineWithID(id, "recency").iterator.flatMap { recency =>
            baselineWithID(id, "frequency").iterator.flatMap { frequency =>
              baselineWithID(id, "monetary").iterator.map { monetary =>
                val indexR = baselineWithID(id, "recency").indexOf(recency)
                val indexF = baselineWithID(id, "frequency").indexOf(frequency)
                val indexM = baselineWithID(id, "monetary").indexOf(monetary)
                var level = ""
                level = if (levelRecency <= indexR) level + "0" else level + "1"
                level = if (levelFrequency <= indexF) level + "0" else level + "1"
                level = if (levelMonetary <= indexM) level + "0" else level + "1"
                (id, recency, frequency, monetary, level, levelCnt)
              }
            }
          }
        }
      }
      .toDF("task_id", "recency", "frequency", "monetary", "level_cd", "level_cnt")
      .groupBy("task_id", "recency", "frequency", "monetary", "level_cd")
      .agg(sum("level_cnt").as("level_cnt"))
      .withColumn("level_rank", levelRankUdf(col("level_cd")))
      .withColumn("level_percent", levelPercentUdf(col("task_id"), col("level_cnt")))
      .select("task_id", "recency", "frequency", "monetary", "level_rank", "level_cd", "level_cnt", "level_percent")
    // pinLevelDF save to hdfs
    val pinLevelFile = "pinLevelDF.parquet"
    logger.info(s"rfm-log: pinLevel path: $tempDir/$pinLevelFile")
    pinLevelDF.repartition(1).write.mode(SaveMode.Overwrite).parquet(s"$tempDir/$pinLevelFile")
    logger.info(s"rfm-log: pinLevel written successfully")

    // 7 * 7 * 7 * 8 = 2744
    val pinLevelRes = pinLevelDF
      .as[(Long, Double, Double, Double, Int, String, Long, Double)]
      .map { case (id, recency, frequency, monetary, levelRank, levelCd, levelCnt, levelPercent) =>
        (id, LevelCrowdResult(recency, frequency, monetary, levelRank, levelCd, levelCnt, levelPercent))
      }.rdd.groupByKey().collect()
      .map { case (id, levelCrowd) => (id, levelCrowd.toArray) }.toMap

    val orderTaskResult = orderTaskGroup.map { task =>
      RfmAnalysisTaskResult(task.taskId, task.startDate, task.endDate,
        dataSizeWithID.getOrElse(task.taskId, 0L), validDataSizeWithID.getOrElse(task.taskId, 0L),
        orderCountWithID.getOrElse(task.taskId, 0L), orderPercentWithID(task.taskId), reOrderPercentWithID.getOrElse(task.taskId, 0),
        IndicatorGeneralResult(meanRFMC(task.taskId, "recency"), medianRFMC(task.taskId, "recency")),
        IndicatorGeneralResult(meanRFMC(task.taskId, "frequency"), medianRFMC(task.taskId, "frequency")),
        IndicatorGeneralResult(meanRFMC(task.taskId, "monetary"), medianRFMC(task.taskId, "monetary")),
        IndicatorGeneralResult(meanRFMC.getOrElse((task.taskId, "cycle"), -1.0), medianRFMC.getOrElse((task.taskId, "cycle"), -1.0)),
        groupBucketAllMap(task.taskId, "recency"), groupBucketAllMap(task.taskId, "frequency"), groupBucketAllMap(task.taskId, "monetary"),
        groupBucketAllMap.getOrElse((task.taskId, "cycle"), Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1))),
        groupBucketAllMap(task.taskId, "amount"), groupBucketAllMap(task.taskId, "channel"),
        pinLevelRes(task.taskId))
    }

    val noOrderTaskResult = noOrderTaskGroup.map { task =>
      RfmAnalysisTaskResult(task.taskId, task.startDate, task.endDate, dataSizeWithID.getOrElse(task.taskId, 0L), validDataSizeWithID.getOrElse(task.taskId, 0L),
        orderCountWithID.getOrElse(task.taskId, 0L), orderPercentWithID.getOrElse(task.taskId, 0D), reOrderPercentWithID.getOrElse(task.taskId, 0),
        IndicatorGeneralResult(-1, -1), IndicatorGeneralResult(-1, -1), IndicatorGeneralResult(-1, -1), IndicatorGeneralResult(-1, -1),
        Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)), Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)), Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)),
        Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)), Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)), Array(IndicatorBucketResult(-1, "-1", -1L, -1, -1, -1)),
        Array(LevelCrowdResult(-1, -1, -1, -1, "-1", -1L, -1)))
    }

    (orderTaskResult ++ noOrderTaskResult, meanRFMC, medianRFMC)
  }

  def compute(spark: SparkSession,
              config: RfmAnalysisConfigInfo,
              taskGroup: Array[RfmAnalysisTaskNew]): Unit = {
    val (taskResult, _, _) = computeImpl(spark, config.jfsBucket, taskGroup)

    ResultSaveUtils.saveToES(spark, config, taskResult)
    logger.info("rfm-log: saveToES finish")
  }

  def main(args: Array[String]): Unit = {

    val config = RfmAnalysisConfigInfo("easy-audience",
      "ads_polaris_analysis_rfm_purchase_behavior_info/assets",
      "ads_polaris_analysis_rfm_purchase_duration_distribution/trend",
      "ads_polaris_analysis_rfm_purchase_frequency_distribution/trend",
      "ads_polaris_analysis_rfm_repurchase_cycle_distribution/trend",
      "ads_polaris_analysis_rfm_monetary_distribution/trend",
      "ads_polaris_analysis_rfm_purchase_amount_distribution/trend",
      "ads_polaris_analysis_rfm_purchase_channel_distribution/trend",
      "ads_polaris_analysis_rfm_consumer_layer/assets")
    val taskGroup = Array(
      //      RfmAnalysisTask(231, "", "2020-12-02", "2020-12-31", 2, "labeldatasets/231_dataset_det", ConfigProperties.JSS_ENDPOINT, ConfigProperties.JSS_ACCESSKEY, ConfigProperties.JSS_SECRETKEY)
      //      RfmAnalysisTask(2, "", "2020-12-02", "2020-12-31", 2, "hdfs://ns1016/user/jd_ad/ads_polaris/niuhuiqian/rfm/ea/input/rfmuser_20201115_parquet"),
      //      RfmAnalysisTask(3, "", "2020-12-02", "2020-12-31", 2, "hdfs://ns1016/user/jd_ad/ads_polaris/niuhuiqian/rfm/ea/input/rfmuser_20201116_parquet"),
      //      RfmAnalysisTask(4, "", "2020-12-02", "2020-12-31", 2, "hdfs://ns1016/user/jd_ad/ads_polaris/niuhuiqian/rfm/ea/input/rfmuser_20201118_parquet"),
      //      RfmAnalysisTask(5, "", "2020-12-02", "2020-12-31", 2, "hdfs://ns1016/user/jd_ad/ads_polaris/niuhuiqian/rfm/ea/input/rfmuser_20201119_parquet"),
      //      RfmAnalysisTask(6, "", "2020-12-02", "2020-12-31", 2, "hdfs://ns1016/user/jd_ad/ads_polaris/niuhuiqian/rfm/ea/input/rfmuser_20201120_parquet")
    )

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RfmAnalysis")
      .config("es.nodes", "http://es-nlb-es-ekrtul421o.jvessel-open-jdstack.jdcloud.local")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true")
      .config("es.nodes.discovery", "true")
      .config("es.batch.size.entries", "5000")
      .enableHiveSupport()
      .getOrCreate()

    val startTime = System.currentTimeMillis()
    compute(spark, config, null)
    val endTime = System.currentTimeMillis()
    val between = endTime - startTime
    val min = between / 1000 / 60
    println(s"rfm-log: minute: $min")

  }

}
