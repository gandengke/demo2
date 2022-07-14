package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo

case class RfmAnalysisConfigInfo(
                              jfsBucket: String,
                              purchaseBehaviorInfo: String,
                              purchaseDurationDistribution: String,
                              purchaseFrequencyDistribution: String,
                              repurchaseCycleDistribution: String,
                              monetaryDistribution: String,
                              purchaseAmountDistribution: String,
                              purchaseChannelDistribution: String,
                              consumerLayer: String
                            )

