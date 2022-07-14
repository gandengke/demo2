package com.jd.easy.audience.task.plugin.step.dataset.udd.engine

import java.util

import com.jd.easy.audience.common.constant.NumberConstant
import com.jd.easy.audience.common.model.UddModel
import com.jd.easy.audience.common.model.param.RuleModel
import com.ql.util.express.ExpressRunner

import scala.collection.JavaConversions._
import scala.collection.immutable.Map

/**
  * 规则解析
  * @param udd UddModel
  */
class RuleEngine(udd: UddModel) extends Serializable {

  /**
    * 规则表达式
    * structure: Map[status, Set[Tuple2(Set[field], exp] ] ]
    */
  private val statusExpTupleSetMap = formatUddRule

  /**
    * 模型规则格式化
    * return Map[status, Set[Tuple2(Set[field], exp] ] ]
    */
  private def formatUddRule: Map[Int, Set[(Set[String], String)]] = {
    val a1ExpTupleSet = formatRuleList(udd.getRule.getA1)
    val a2ExpTupleSet = formatRuleList(udd.getRule.getA2)
    val a3ExpTupleSet = formatRuleList(udd.getRule.getA3)
    val a4ExpTupleSet = formatRuleList(udd.getRule.getA4)
    Map(NumberConstant.INT_1 -> a1ExpTupleSet, NumberConstant.INT_2 -> a2ExpTupleSet, NumberConstant.INT_3 -> a3ExpTupleSet, NumberConstant.INT_4 -> a4ExpTupleSet)
  }

  /**
    * 获取规则集合表达式及特征集合
    * @return Set[(Set[String], String)]，第一层set是或关系
    */
  private def formatRuleList(ruleModelList: util.List[RuleModel]): Set[(Set[String], String)] = {
    val expTupleSet =  scala.collection.mutable.Set[(Set[String], String)]()
    for (ruleModel <- ruleModelList) {
      //e.g. (app_dm_4a_user_order_tb_ord_cnt_14>1) and (app_dm_4a_user_feature_tb_phone_click_14>1)
      val exp = ruleModel.getRuleModelExp
      //某个4a状态规则的所有字段集合
      val fields = ruleModel.getRule.map(_.getFieldName).toSet
      expTupleSet+=((fields, exp))
    }
    expTupleSet.toSet
  }

  /**
    * 规则eval
    * @param runner ExpressRunner
    * @param expTuple (Set[field], exp)
    * @param featureMap Map[field, value])
    * @return
    */
  private def exprEval(runner: ExpressRunner, expTuple: (Set[String], String), featureMap: Map[String, Double]): Boolean = {
    //Tuple2(Set[field], exp)
    //规则表达式
    var evalExp = expTuple._2
    //某个4a状态下配置规则的所有特征字段名
    for (field <- expTuple._1) {
      //使用特征的值替换特征字段e.g 14>6 and 8<6
      evalExp = evalExp.replaceAll(field, featureMap.getOrElse(field, 0D).toString)
    }
    //判断该用户是否满足4a规则
    runner.execute(evalExp, null, null, true, false).asInstanceOf[Boolean]
  }


  /**
    * 根据规则和特征做分层
    * @param expRunner  ExpressRunner
    * @param featureMap Map[field, value])
    * @return Map[status, Map[field, value] ]
    */
  def execute(expRunner: ExpressRunner, featureMap: Map[String, Double]): Map[Int, Map[String, Double]] = {
    val statusFeatureMap = scala.collection.mutable.Map[Int, Map[String, Double]]()
    //statusExpTupleSetMap = Map[status, Set[Tuple2(Set[field], exp)] ] ]
    for ((status, expTupleSet) <- statusExpTupleSetMap) {
      for (expTuple <- expTupleSet) {
        //一个状态下多个规则，多个规则或的关系，一个规则下多个规则表达式，可以或可以且
        if (exprEval(expRunner, expTuple, featureMap)) {
          //用户满足规则，获取规则涉及的特征
          val targetFeatureMap = featureMap.filterKeys(expTuple._1.contains)
          if (statusFeatureMap.contains(status)) {
            statusFeatureMap(status) = statusFeatureMap(status) ++ targetFeatureMap
          } else {
            statusFeatureMap += (status-> targetFeatureMap)
          }
        }
      }
    }

    statusFeatureMap.toMap
  }

}
