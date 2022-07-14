package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.result

case class IndicatorBucketResult(
                                  rank: Int,
                                  interval: String,
                                  count: Long,
                                  upBorder: Double,
                                  lowBorder: Double,
                                  percent: Double
                                )