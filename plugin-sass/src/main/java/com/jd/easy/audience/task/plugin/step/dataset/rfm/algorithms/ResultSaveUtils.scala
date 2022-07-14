package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms

import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.result.{IndicatorBucketResult, LevelCrowdResult, RfmAnalysisTaskResult}
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo.RfmAnalysisConfigInfo
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

object ResultSaveUtils {

  def saveToES(spark: SparkSession,
               config: RfmAnalysisConfigInfo,
               taskResult: Array[RfmAnalysisTaskResult]): Unit = {

    import spark.implicits._

    val purchaseBehaviorInfo = config.purchaseBehaviorInfo
    val purchaseDurationDistribution = config.purchaseDurationDistribution
    val purchaseFrequencyDistribution = config.purchaseFrequencyDistribution
    val repurchaseCycleDistribution = config.repurchaseCycleDistribution
    val monetaryDistribution = config.monetaryDistribution
    val purchaseAmountDistribution = config.purchaseAmountDistribution
    val purchaseChannelDistribution = config.purchaseChannelDistribution
    val consumerLayer = config.consumerLayer


    /**
     * id：
     * 人群规模、行为概览表id: analyseId
     * 最近购买分布时长id: analyseId + recencyRank
     * 购买频次分布id: analyseId + frequencyRank
     * 复购周期分布id: analyseId + cycleRank
     * 消费金额分布id: analyseId + monetaryRank
     * 消费件数分布id: analyseId + amountRank
     * 购买渠道分布id: analyseId + channelRank
     * 用户分层分析id: analyseId + recencyBaseValue + frequencyBaseValue + monetaryBaseValue + levelRank
     */

    // ES
    /**
     * 购买人群规模+购买行为概览
     * 字段名	            数据类型	            字段描述	              对应上侧结果数据字段
     * analyseId	        long	              分析ID	                taskId
     * endDate     	    date	              结束日期	              endDate
     * dataSize	        long	              该任务的数据条数	        dataSize
     * validDataSize     long                该任务的有效数据条数     validDataSize
     * orderSize	        long	              该人群包的购买人数     	orderSize
     * purchasePercent	  double            	该人群包的购买占比	      orderPercent
     * reOrderPercent	  double	          	该人群包的复购率      	  reOrderPercent
     * recencyMean	      double	          	最近购买时长均值	        recencyGeneral / mean
     * recencyMedian    	double	          	最近购买时长中位数	      recencyGeneral / median
     * frequencyMean	    double	           	购买频次均值	          frequencyGeneral / mean
     * frequencyMedian 	double	           	购买频次中位数       	  frequencyGeneral / median
     * cycleMean	        double	          	复购周期均值	          cycleGeneral / mean
     * cycleMedian      	double	          	复购周期中位数        	cycleGeneral / median
     * monetaryMean	    double	          	消费金额均值          	monetaryGeneral / mean
     * monetaryMedian	  double	          	消费金额中位数        	monetaryGeneral / median
     */

    val taskGeneralOverview = taskResult.map { case RfmAnalysisTaskResult(taskId, startDate, endDate, dataSize, validDataSize,
    orderSize, orderPercent, reOrderPercent, recencyGeneral, frequencyGeneral, monetaryGenaral, cycleGeneral, _, _, _, _, _, _, _) =>
      (taskId, startDate, endDate, dataSize, validDataSize, orderSize, orderPercent, reOrderPercent,
        recencyGeneral.mean, recencyGeneral.median,
        frequencyGeneral.mean, frequencyGeneral.median,
        cycleGeneral.mean, cycleGeneral.median,
        monetaryGenaral.mean, monetaryGenaral.median)
    }.toSeq.toDF("analyseId", "startDate", "endDate", "dataSize", "validDataSize", "orderSize", "purchasePercent", "reOrderPercent",
      "recencyMean", "recencyMedian", "frequencyMean", "frequencyMedian", "cycleMean", "cycleMedian", "monetaryMean", "monetaryMedian")
      .withColumn("id", col("analyseId"))
    EsSparkSQL.saveToEs(taskGeneralOverview, purchaseBehaviorInfo, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $purchaseBehaviorInfo write successfully")

    /**
     * 最近购买时长分布
     * 字段名	            数据类型	            字段描述	            对应上侧结果数据字段
     * analyseId        	long              	分析ID		            taskId
     * startDate       	date		            开始日期		          startDate
     * endDate         	date		            结束日期		          endDate
     * recencyMean      	double	            最近购买时长均值	      recencyGeneral / mean
     * recencyMedian    	double		          最近购买时长中位数	    recencyGeneral / median
     * recencyRank     	int		              区间序号            	recencyBucket / rank
     * recencyInterval  	keyword	           	区间	                recencyBucket / interval
     * recencyCount    	long		            落在该区间的人数	      recencyBucket / count
     * recencyUpBorder 	double	          	该区间上限	          recencyBucket / upBorder
     * recencyLowBorder	double		          该区间下限	          recencyBucket / lowBorder
     * recencyPercent  	double		          该区间人群占比	      recencyBucket / percent
     */
    val recencyDistribution = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, recencyGeneral, _, _, _, recencyBucket, _, _, _, _, _, _) =>
      recencyBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, upBorder, lowBorder, percent) =>
        (taskId, startDate, endDate, recencyGeneral.mean, recencyGeneral.median,
          rank, interval, count, upBorder, lowBorder, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "recencyMean", "recencyMedian",
      "recencyRank", "recencyInterval", "recencyCount", "recencyUpBorder", "recencyLowBorder", "recencyPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("recencyRank")))
    EsSparkSQL.saveToEs(recencyDistribution, purchaseDurationDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $purchaseDurationDistribution write successfully")

    val frequencyDistribution = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, frequencyGeneral, _, _, _, frequencyBucket, _, _, _, _, _) =>
      frequencyBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, upBorder, lowBorder, percent) =>
        (taskId, startDate, endDate, frequencyGeneral.mean, frequencyGeneral.median,
          rank, interval, count, upBorder, lowBorder, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "frequencyMean", "frequencyMedian",
      "frequencyRank", "frequencyInterval", "frequencyCount", "frequencyUpBorder", "frequencyLowBorder", "frequencyPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("frequencyRank")))
    EsSparkSQL.saveToEs(frequencyDistribution, purchaseFrequencyDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $purchaseFrequencyDistribution write successfully")

    val monetaryDistributionDF = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, _, monetaryGenaral, _, _, _, monetaryBucket, _, _, _, _) =>
      monetaryBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, upBorder, lowBorder, percent) =>
        (taskId, startDate, endDate, monetaryGenaral.mean, monetaryGenaral.median,
          rank, interval, count, upBorder, lowBorder, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "monetaryMean", "monetaryMedian",
      "monetaryRank", "monetaryInterval", "monetaryCount", "monetaryUpBorder", "monetaryLowBorder", "monetaryPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("monetaryRank")))
    EsSparkSQL.saveToEs(monetaryDistributionDF, monetaryDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $monetaryDistribution write successfully")

    val cycleDistribution = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, _, _, cycleGeneral, _, _, _, cycleBucket, _, _, _) =>
      cycleBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, upBorder, lowBorder, percent) =>
        (taskId, startDate, endDate, cycleGeneral.mean, cycleGeneral.median,
          rank, interval, count, upBorder, lowBorder, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "cycleMean", "cycleMedian",
      "cycleRank", "cycleInterval", "cycleCount", "cycleUpBorder", "cycleLowBorder", "cyclePercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("cycleRank")))
    EsSparkSQL.saveToEs(cycleDistribution, repurchaseCycleDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $repurchaseCycleDistribution write successfully")

    val amountDistribution = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, _, _, _, _, _, _, _, amountBucket, _, _) =>
      amountBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, upBorder, lowBorder, percent) =>
        (taskId, startDate, endDate, rank, interval, count, upBorder, lowBorder, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate",
      "amountRank", "amountInterval", "amountCount", "amountUpBorder", "amountLowBorder", "amountPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("amountRank")))
    EsSparkSQL.saveToEs(amountDistribution, purchaseAmountDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $purchaseAmountDistribution write successfully")

    val channelDistribution = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, channelBucket, _) =>
      channelBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
        (taskId, startDate, endDate, rank, interval, count, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "channelRank", "channelInterval", "channelCount", "channelPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("channelRank")))
    EsSparkSQL.saveToEs(channelDistribution, purchaseChannelDistribution, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $purchaseChannelDistribution write successfully")

    /**
     * 用户分层分析
     * 字段名	              数据类型	            字段描述	            对应上侧结果数据字段
     * analyseId	          long	              分析ID              	taskId
     * startDate         	date	              开始日期             	startDate
     * endDate	            date	              结束日期             	endDate
     * recencyBaseValue	  double            	最近购买时长基准值	    levelCrowd / recency
     * frequencyBaseValue	double            	购买频次基准值	      levelCrowd / frequency
     * monetaryBaseValue	  double	            消费金额基准值       	levelCrowd / monetary
     * levelRank	          int	                人群包序号（1-8）    	levelCrowd / rank
     * level	              keyword	            人群包描述           	levelCrowd / level
     * levelCount        	long	              该人群包的人数	      levelCrowd / count
     * levelPercent       	double	            该人群包人数占比	      levelCrowd / percent
     */
    val levelCrowdAnalysis = taskResult.flatMap { case RfmAnalysisTaskResult(taskId, startDate, endDate, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, levelCrowd) =>
      levelCrowd.iterator.map { case LevelCrowdResult(recency, frequency, monetary, rank, level, count, percent) =>
        (taskId, startDate, endDate, recency, frequency, monetary, rank, level, count, percent)
      }
    }.toSeq.toDF("analyseId", "startDate", "endDate", "recencyBaseValue", "frequencyBaseValue", "monetaryBaseValue",
      "levelRank", "level", "levelCount", "levelPercent")
      .withColumn("id", concat_ws("-", col("analyseId"), col("recencyBaseValue"), col("frequencyBaseValue"), col("monetaryBaseValue"), col("levelRank")))
    EsSparkSQL.saveToEs(levelCrowdAnalysis, consumerLayer, Map("es.mapping.id" -> "id"))
    println(s"rfm-log: $consumerLayer write successfully")
  }

  def saveAsCSV(spark: SparkSession,
                taskResult: Array[RfmAnalysisTaskResult],
                mean: Map[(Long, String), Double],
                median: Map[(Long, String), Double],
                dir: String): Unit = {

    import spark.implicits._
    val outputDir = dir.stripSuffix("/")
    mean.map { case ((id, indicator), avg) =>
      (id, indicator, avg, median.getOrElse((id, indicator), -1.0))
    }.toSeq.toDF("任务ID", "指标", "均值", "中位数")
      .orderBy("任务ID", "指标")
      .coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(s"$outputDir/avg_median.csv")

    taskResult.flatMap { case RfmAnalysisTaskResult(taskId, _, _, _, _,
    _, _, _, _, _, _, _, recencyBucket, frequencyBucket, monetaryBucket, cycleBucket, amountBucket, channelBucket, _) =>
      recencyBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
        (taskId, "recency", rank, interval, count, percent)
      } ++
        frequencyBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
          (taskId, "frequency", rank, interval, count, percent)
        } ++
        monetaryBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
          (taskId, "monetary", rank, interval, count, percent)
        } ++
        cycleBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
          (taskId, "cycle", rank, interval, count, percent)
        } ++
        amountBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
          (taskId, "amount", rank, interval, count, percent)
        } ++
        channelBucket.iterator.map { case IndicatorBucketResult(rank, interval, count, _, _, percent) =>
          (taskId, "channel", rank, interval, count, percent)
        }
    }.toSeq.toDF("任务ID", "指标", "区件序号", "区间", "人数", "人数占比")
      .orderBy("任务ID", "指标", "区件序号")
      .coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(s"$outputDir/bucket.csv")
  }

}
