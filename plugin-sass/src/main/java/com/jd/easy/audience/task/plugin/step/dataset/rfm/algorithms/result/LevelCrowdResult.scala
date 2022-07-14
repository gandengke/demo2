package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.result

case class LevelCrowdResult(
                             recency: Double,
                             frequency: Double,
                             monetary: Double,
                             rank: Int,
                             level: String,
                             count: Long,
                             percent: Double
                           )

