package com.ggc.index_calc.top10.acc

import com.ggc.index_calc.top10.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.HashMap

class CategoryAcc extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]] {

  var map: Map[(String, String), Long] = new HashMap[(String, String), Long]

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {

    val categoryAcc = new CategoryAcc
    categoryAcc.map ++= map
    categoryAcc
  }

  override def reset(): Unit = Map[(String, String), Long]()

  override def add(userVisitAction: UserVisitAction): Unit = {

    if (userVisitAction.click_category_id != -1) {
      map += (userVisitAction.click_category_id.toString, "click") -> (map.getOrElse((userVisitAction.click_category_id.toString, "click"), 0L) + 1L)
    } else if (userVisitAction.order_category_ids != "null") {
      userVisitAction.order_category_ids.split(",").foreach(cid => {
        map += (((cid.toString, "order"), map.getOrElse((cid.toString, "order"), 0L) + 1))
      })
    } else if (userVisitAction.pay_category_ids != "null") {
      userVisitAction.pay_category_ids.split(",").foreach(cid => {
        map += (((cid.toString, "pay"), map.getOrElse((cid.toString, "pay"), 0L) + 1))
      })
    }

  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    val categoryAcc = other.asInstanceOf[CategoryAcc]

    categoryAcc.map.foreach {
      case (cid2action, count:Long) => map += ((cid2action, map.getOrElse(cid2action, 0L) + count))
      case _ =>
    }
  }

  override def value: Map[(String, String), Long] = map
}
