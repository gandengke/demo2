package com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

case class RfmAnalysisTask(taskId: Long,
                           outputPath: String,
                           startDate: String,
                           endDate: String,
                           dataType: Int,
                           dataPath: String,
                           endPoint: String,
                           accessKey: String,
                           secretKey: String
                          )

object RfmAnalysisTask {

  def load(path: String): Array[RfmAnalysisTask] = {
    val fs = FileSystem.get(new Configuration())
    val mapper = new ObjectMapper
    val root = mapper.readTree(fs.open(new Path(path)))

    val taskGroup = ArrayBuffer.empty[RfmAnalysisTask]
    val it = root.path("taskSets").iterator()

    while (it.hasNext) {
      val node = it.next()
      val taskId = node.path("taskId").asLong()
      val outputPath = node.path("outputPath").asText()
      val startDt = node.path("startDate").asText()
      val endDt = node.path("endDate").asText()
      val dataType = node.path("taskType").asInt()
      val dataPath = node.path("params").asText()
      val endPoint = node.path("endPoint").asText()
      val accessKey = node.path("accessKey").asText()
      val secretKey = node.path("secretKey").asText()
      println(s"rfm-log: $taskId, $outputPath, $endDt, $dataType, $dataPath, $endPoint, $accessKey, $secretKey")
      val taskInfo = RfmAnalysisTask(taskId, outputPath, startDt, endDt, dataType, dataPath, endPoint, accessKey, secretKey)
      taskGroup.append(taskInfo)
    }
    taskGroup.toArray
  }
}