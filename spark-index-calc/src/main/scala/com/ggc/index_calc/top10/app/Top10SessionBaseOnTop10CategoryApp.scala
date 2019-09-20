package com.ggc.index_calc.top10.app

import com.ggc.index_calc.top10.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable

/**
 * Top10热门品类中每个品类的 Top10 活跃 Session 统计
 *
 * 前推+后推并举法
 */
//noinspection DuplicatedCode
object Top10SessionBaseOnTop10CategoryApp extends App {


  /**
   * 实现方式一：
   * 关键点：每个cid的sid按clickcount排序，
   * 排序过程中要把对应数据都加载到内存中，如果数据量大会OOM
   * *
   * List[CategoryCountInfo(cid,clickCount,_,_)]
   * parallelize=> RDD[(cid,1)]
   * +
   * RDD[UserVisitAction]
   * filter+map=> RDD[(cid,sid)]
   * ⏬
   * RDD[(cid,1)] join RDD[(cid,sid)]
   * => RDD[(cid:String, (sid:String, 1:Int))]
   * ⏬
   * map=> RDD[((cid:String, sid:String), 1:Int)]
   * ⏬
   * reduceByKey => RDD[((cid:String, sid:String), count:Int)]
   * map=>RDD[(cid:String, (sid:String, count:Int))]
   * ⏬
   * groupByKey=>RDD[(cid, Iterable[(sid:String, count:Int)])]
   * map=>RDD[(String, List[CategorySession])]
   * ⏬
   * 最终结果RDD[(String, List[CategorySession])]
   *
   */
  def statCategoryTop10Session_Means1(sc: SparkContext
                                      , categoryTop10Result: Array[CategoryCountInfo]
                                      , userVisitActionRDD: RDD[UserVisitAction]) = {

    val cid2countRDD = sc.parallelize(categoryTop10Result)
      .map(categoryCountInfo => (categoryCountInfo.categoryId, 1))

    val cid2sidRDD = userVisitActionRDD
      .filter(_.click_category_id != -1)
      .map(userVisitAction => (userVisitAction.click_category_id.toString, userVisitAction.session_id))

    val resultRDD =
      cid2sidRDD
        .join(cid2countRDD) //RDD[(String, (String, Int))]
        .map(t => ((t._1, t._2._1), t._2._2)) //RDD[((cid:String, sid:String), 1:Int)]
        .reduceByKey(_ + _) //RDD[((cid:String, sid:String), count:Int)]
        .map(t => (t._1._1, (t._1._2, t._2))) //RDD[(cid:String, (sid:String, count:Int))]
        .groupByKey() //RDD[(cid, Iterable[(sid:String, count:Int)])]
        .map {
          case (cid, it) => (cid,
            it.toList.sortBy(t => t._2)(Ordering.Int.reverse).take(10)
              .map {
                case (sid, count) => CategorySession(cid, sid, count)
              })
        } //  RDD[(String, List[(String, Int)])]


    resultRDD
  }


  /**
   * 实现方式二：
   * 关键点：排序是通过RDD的sortBy整体排序，排序过程不会导致OOM，
   * 但是取出每个cid对应Top10时，也需要把cid对应的所有数据都加载到内存 ？？待确认是这个原因
   *
   */
  def statCategoryTop10Session_Means2(sc: SparkContext
                                      , categoryTop10Result: Array[CategoryCountInfo]
                                      , userVisitActionRDD: RDD[UserVisitAction]) = {

    val cid2countRDD = sc.parallelize(categoryTop10Result)
      .map(categoryCountInfo => (categoryCountInfo.categoryId, 1))

    val cid2sidRDD = userVisitActionRDD
      .filter(_.click_category_id != -1)
      .map(userVisitAction => (userVisitAction.click_category_id.toString, userVisitAction.session_id))

    val resultRDD =
      cid2sidRDD
        .join(cid2countRDD)
        .map(t => ((t._1, t._2._1), t._2._2)) //RDD[((cid:String, sid:String), 1:Int)]
        .reduceByKey(_ + _) //RDD[((cid:String, sid:String), count:Int)]
        .map(t => (t._1._1, t._1._2, t._2))
        .sortBy(t => (t._1, t._3), ascending = false)
        .map(t => (t._1, (t._2, t._3)))
        .groupByKey()
        .map {
          case (cid, it) => (cid, it.toList.take(10).map {
            case (sid, count) => CategorySession(cid, sid, count)
          })
        }

    resultRDD
  }


  /**
   * 实现方式三
   */
  def statCategoryTop10Session_Means3(sc: SparkContext
                                      , categoryTop10Result: Array[CategoryCountInfo]
                                      , userVisitActionRDD: RDD[UserVisitAction]) = {
    val cid2SidCountIter = userVisitActionRDD
      .filter(userVisitAction => { // 包含top10cid的那些用户点击记录过滤出来
        categoryTop10Result.map(_.categoryId).contains(userVisitAction.click_category_id.toString)
      })
      .map(userVisitAction => ((userVisitAction.click_category_id.toString, userVisitAction.session_id), 1))
      .reduceByKey(_ + _)
      .map(t => (t._1._1, (t._1._2, t._2))) //RDD[(cid:String, (sid:String, count:Int))]
      .groupByKey() //RDD[(cid, Iterable[(sid:String, count:Int)])]

    categoryTop10Result
      .map(_.categoryId)
      .map(cid => {
        val categorySessions = cid2SidCountIter
          .filter(_._1 == cid)
          .flatMap(_._2)
          .sortBy(_._2, ascending = false)
          .take(10)
          .map {
            case (sid, count) => CategorySession(cid, sid, count)
          }.toList
        (cid, categorySessions)
      })

  }

  /**
   * 实现方式四
   */
  def statCategoryTop10Session_Means4(sc: SparkContext
                                      , categoryTop10Result: Array[CategoryCountInfo]
                                      , userVisitActionRDD: RDD[UserVisitAction]) = {

    val cid2SidCountIter = userVisitActionRDD
      .filter(userVisitAction => { // 包含top10cid的那些用户点击记录过滤出来
        categoryTop10Result.map(_.categoryId).contains(userVisitAction.click_category_id.toString)
      })
      .map(userVisitAction => ((userVisitAction.click_category_id.toString, userVisitAction.session_id), 1))
      .reduceByKey(new CategoryPartitioner(categoryTop10Result), _ + _) // 重新分区
      .map {
        case ((cid, sid), count) => (cid,CategorySession(cid.toString, sid, count))
      }

    val resultRDD = cid2SidCountIter.mapPartitions(iter => {
      var treeSet: mutable.TreeSet[CategorySession] = new mutable.TreeSet[CategorySession]()
      iter.foreach(t => {
        treeSet += t._2
        if (treeSet.size > 10) {
          treeSet = treeSet.take(10)
        }
      })
      treeSet.toIterator
    })
    resultRDD
  }

  class CategoryPartitioner(categoryTop10Result: Array[CategoryCountInfo]) extends Partitioner {

    val cid2index: Map[String, Int] = categoryTop10Result.map(_.categoryId).zipWithIndex.toMap

    override def numPartitions: Int = categoryTop10Result.length

    override def getPartition(key: Any): Int = {
      key match {
        case (cid, _) => cid2index(cid.toString)
      }
    }
  }

}


