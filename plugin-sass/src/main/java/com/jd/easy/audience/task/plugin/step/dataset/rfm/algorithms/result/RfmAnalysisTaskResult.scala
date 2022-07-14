package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.result

case class RfmAnalysisTaskResult(taskId: Long,
                                 startDate: String,
                                 endDate: String,
                                 dataSize: Long,
                                 validDataSize: Long,
                                 orderSize: Long,
                                 orderPercent: Double,
                                 reOrderPercent: Double,
                                 recencyGeneral: IndicatorGeneralResult,
                                 frequencyGeneral: IndicatorGeneralResult,
                                 monetaryGenaral: IndicatorGeneralResult,
                                 cycleGeneral: IndicatorGeneralResult,
                                 recencyBucket: Array[IndicatorBucketResult],
                                 frequencyBucket: Array[IndicatorBucketResult],
                                 monetaryBucket: Array[IndicatorBucketResult],
                                 cycleBucket: Array[IndicatorBucketResult],
                                 amountBucket: Array[IndicatorBucketResult],
                                 channelBucket: Array[IndicatorBucketResult],
                                 levelCrowd: Array[LevelCrowdResult])

