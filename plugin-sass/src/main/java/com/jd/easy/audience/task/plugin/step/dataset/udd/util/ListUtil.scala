package com.jd.easy.audience.task.plugin.step.dataset.udd.util

import scala.collection.immutable.Map

/**
  * List操作工具类
  */
object ListUtil {

  /**
    * 合并list[Int] -> Map[Int, value]
    *
    * @param featureList featureList
    * @return
    */
  def mergeCodeList(featureList: Seq[Int]): Map[Int, Long] = {
    val touchMap = scala.collection.mutable.Map[Int, Long]()
    for (code <- featureList) {
      touchMap(code) = touchMap.getOrElse(code, 0L) + 1L
    }
    touchMap.toMap
  }

  /**
    * 合并mapList Map[Int, AnyVal(Long)]
    *
    * @param touchSpotList touchSpotList
    * @return
    */
  def mergeTouchSpotList(touchSpotList: Seq[Map[Int, AnyVal]]): Map[Int, Long] = {
    var result: Map[Int, Long] = Map()

    for (touchSpot <- touchSpotList) {
      if (null != touchSpot && touchSpot.nonEmpty) {
        result = result ++ collection.mutable.Map(touchSpot.toSeq: _*).map(t => t._1 -> (t._2.toString.toLong + result.getOrElse(t._1, 0L)))
      }
    }
    result.filter(t => t._2 > 0)
  }

}
