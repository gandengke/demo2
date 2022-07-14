package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo

import org.apache.spark.sql.{Dataset, Row}

case class RfmAnalysisTaskNew(taskId: Long,
                              outputPath: String,
                              startDate: String,
                              endDate: String,
                              dataType: Int,
                              dataSet: Dataset[Row],
                              endPoint: String,
                              accessKey: String,
                              secretKey: String
                          )

