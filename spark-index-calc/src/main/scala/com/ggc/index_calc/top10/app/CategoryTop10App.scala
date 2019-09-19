package com.ggc.index_calc.top10.app

import com.ggc.index_calc.top10.acc.CategoryAcc
import com.ggc.index_calc.top10.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * 统计Top10 热门品类
 */
//noinspection DuplicatedCode
object CategoryTop10App {


  /**
   * 分别统计 每个品类点击的次数, 下单的次数和支付的次数
   */
  def statCategoryTop10_Means1(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {


    /**
     * 具体步骤：
     *  1.遍历RDD中每个UserVisitAction，筛选出click_category_id属性不为空的记录，
     *  2.对click_category_id分组，分组中统计组内的总数
     *  3.按点击次数降序排序，取前10个
     *
     * 下单的次数和支付的次数与点击的次数步骤类似
     */

    //1.遍历RDD中每个UserVisitAction，筛选出click_category_id属性不为空的记录，
    val clickRDD = userVisitActionRDD.filter(userVisitAction => {
      userVisitAction.click_category_id != -1
    })
      .map(userVisitAction => (userVisitAction.click_category_id.toString, 1))
      .reduceByKey(_ + _)

    //下单的次数
    val orderRDD = userVisitActionRDD
      .filter(userVisitAction => {
        userVisitAction.order_category_ids != "null"
      })
      .flatMap(userVisitAction => {
        val categoryId2Count = userVisitAction.order_category_ids.split(",").map(id => (id, 1))
        categoryId2Count
      })
      .reduceByKey(_ + _)


    //支付的次数
    val payRDD = userVisitActionRDD
      .filter(userVisitAction => {
        userVisitAction.pay_category_ids != "null"
      })
      .flatMap(userVisitAction => {
        val payId2Count = userVisitAction.pay_category_ids.split(",").map(id => (id, 1))
        payId2Count
      })
      .reduceByKey(_ + _)

    val resultRDD = clickRDD
      .join(orderRDD)
      .join(payRDD)
      .map {
        case (t11, ((t21, t22), t23)) => (t11, (t21, t22, t23))
      }
      .sortBy(t => t._2, ascending = false)
    //      .sortBy(t => t._2, ascending = false)(
    //        Ordering.Tuple3(
    //          Ordering.Int.reverse,
    //          Ordering.Int.reverse,
    //          Ordering.Int.reverse),
    //        ClassTag.Any)

    resultRDD //(2,(6119,1767,1196))
      .take(10)
      .map {
        case (cid, (clickCount, orderCount, payCount)) =>
          CategoryCountInfo(
            cid.toString,
            clickCount,
            orderCount,
            payCount)
      }// CategoryCountInfo(15,6120,1672,1259)



  }
  /**
   * 只需要对原始数据遍历一次，就可以 统计每个品类点击的次数, 下单的次数和支付的次数
   */
  def statCategoryTop10_Means2(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {


    val resultRDD =
      userVisitActionRDD
        .flatMap(userVisitAction => {
          val ab = new ArrayBuffer[((Long, String), Int)]()
          if (userVisitAction.click_category_id != -1) {
            ab += (((userVisitAction.click_category_id, "click"), 1))
          } else if (userVisitAction.order_category_ids != "null") {
            userVisitAction.order_category_ids.split(",").foreach(cid => {
              ab += (((cid.toLong, "order"), 1))
            })
          } else if (userVisitAction.pay_category_ids != "null") {
            userVisitAction.pay_category_ids.split(",").foreach(cid => {
              ab += (((cid.toLong, "pay"), 1))
            })
          }
          ab
        })
        .reduceByKey(_ + _) // ((2,pay),1196)
        .map(t => (t._1._1, (t._1._2, t._2))) //(2,(pay,1196))
        .groupByKey() // (13,CompactBuffer((pay,1161), (order,1781), (click,6036)))
        .map(t => {
          (t._1, t._2.toList.sortBy(t => t._1).map(f => f._2) match {
            case List(clickCount, orderCount, payCount) => (clickCount, orderCount, payCount)
          })
        })
        .sortBy(t => t._2, ascending = false)


    resultRDD //(2,(6119,1767,1196))
      .take(10)
      .map {
        case (cid, (clickCount, orderCount, payCount)) =>
          CategoryCountInfo(
            cid.toString,
            clickCount,
            orderCount,
            payCount)
      } // CategoryCountInfo(15,6120,1672,1259)


  }


  /**
   * 利用累加器的多个task共享写操作，并在driver端归总和统计最后的结果
   *
   */
  def statCategoryTop10_Means3(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {

    val categoryAcc = new CategoryAcc
    sc.register(categoryAcc)

    userVisitActionRDD.foreach(userVisitAction => {
      categoryAcc.add(userVisitAction)
    })

    val result =
      categoryAcc.value //Map[(String, String), Long]
        .groupBy(_._1._1) //Map[String, Map[(String, String), Long]]
        .map {
          case (cid, map) =>
            CategoryCountInfo(
              cid,
              map.getOrElse((cid, "click"), 0L),
              map.getOrElse((cid, "order"), 0L),
              map.getOrElse((cid, "pay"), 0L)
            )
        }//immutable.Iterable[CategoryCountInfo]
        .toList //immutable.Seq[CategoryCountInfo]
        .sortBy(categoryCountInfo =>
          (categoryCountInfo.clickCount, categoryCountInfo.orderCount, categoryCountInfo.payCount))(
          Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse)
        )
        .take(10)

    result
  }

}
