package com.ggc.index_calc.practice1.app

import java.text.DecimalFormat

import com.ggc.index_calc.practice1.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 页面单跳转化率统计
 */
//noinspection DuplicatedCode
object PageConversionApp {

  def calcPageConversion(spark: SparkContext
                         , userVisitActionRDD: RDD[UserVisitAction]
                         , targetPageFlow: String) = {

    /*
      1.根据相应的页面路径，转化成page_id数组
        a)1,2,3,4,5,6,7 => Array(1,2,3,4,5,6,7)
      2.过滤出规定页面的日志记录，并统计出来每个页面的访问次数
        a)countByKey算子
      3.明确哪些页面需要计算跳转次数
        a)1->2,2->3,3->4... ; zip算子
      4.按照session获得统计目标页面的跳转次数
        a)按session分组，然后对每组内的UserVisitAction 按时间升序排序
        b)转换成页面跳转标识（1->2,...）
        c)过滤和统计目标一致的跳转
      5.统计跳转次数
      6.计算跳转率
     */

    //1.根据相应的页面路径，转化成page_id数组
    val pageFlowArr = targetPageFlow.split(",")

    //2.过滤出规定页面的日志记录，并统计出来每个页面的访问次数
    val targetPageCountMap =
      userVisitActionRDD
      .filter(uva => pageFlowArr.contains(uva.page_id.toString))
      .map(uva => (uva.page_id,1)) //RDD[(page_id,1)]
      .countByKey() // Map[page_id,count]

    //3.明确哪些页面需要计算跳转次数
    val prePageFlowArr = pageFlowArr.slice(0, pageFlowArr.length - 1)
    val postPageFlowArr = pageFlowArr.slice(1, pageFlowArr.length)

    val targetJumpPages = prePageFlowArr
      .zip(postPageFlowArr)
      .map(t => t._1 + "->" + t._2)

    //4.按照session获得统计目标页面的跳转次数

    //  a)按session分组，然后对每组内的UserVisitAction 按时间升序排序
    val resultRDD =
      userVisitActionRDD
        .groupBy(uva => uva.session_id) //RDD[(sid,UserVisitAction)]
        .flatMap {
          case (_, uvaIter) =>
            val actionIter = uvaIter
              .toList
              .sortBy(uva => uva.action_time)
              .map(uva => uva.page_id)
            // b)转换成页面跳转标识（1->2,...）
            val init = actionIter.init
            val tail = actionIter.tail
            init
              .zip(tail)
              .map(t => t._1 + "->" + t._2)
              // c)过滤和统计目标一致的跳转
              .filter(pageId => targetJumpPages.contains(pageId))

        }.map((_, 1))
        .reduceByKey(_ + _)

    val pageJumpCount = resultRDD.collect()

    val decimalFormat = new DecimalFormat(".00%")
    // 转换成百分比
    val conversionRate = pageJumpCount.map {
      case (page2page, jumpCount) =>
        val visitCount = targetPageCountMap.getOrElse(page2page.split("->").head.toLong, 0L)
        val rate = decimalFormat.format(jumpCount.toDouble / visitCount)
        (page2page, rate)
    }
    conversionRate.foreach(println)


  }

}
