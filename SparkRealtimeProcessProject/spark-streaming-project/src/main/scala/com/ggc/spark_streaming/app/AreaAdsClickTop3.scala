package com.ggc.spark_streaming.app

import com.ggc.spark_streaming.bean.AdsInfo
import com.ggc.spark_streaming.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * 需求1: 每天每地区热门广告 Top3
 */
object AreaAdsClickTop3 {
  def statHotAdsClick4PerDayArea(adsInfoDStream: DStream[AdsInfo]) = {

     adsInfoDStream
      // ((day, area, adsId), 1)
      .map(adsInfo => {
        ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1)
      })
      .updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
        Some(seq.sum + option.getOrElse(0))
      }) // ((day, area, adsId), 1000)
      .map {
        case ((day, area, adsId), count) => ((day, area), (adsId, count))
      } //((day, area), (adsId, 1000))
      // 分组, 排序, top3
      .groupByKey() //(day, area, List[(adsId, 1000)])
      .map {
        case ((day, area), adsId2CountIt) =>
          (day, area, adsId2CountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
      }//(day, area, List[(adsId, 1000)])
      // 输出到redis中
      .foreachRDD(rdd => {
        //这个函数在Driver端执行

        val client = RedisUtil.getJedisClient
        val dayAreaAdsCountListTupleArr = rdd.collect()
        /*-----------------------方法一 开始---------------------------*/
        // 数据量已经很小, 所以可以在驱动端统一写出
//        dayAreaAdsCountListTupleArr.foreach{
//
//          case (day,area,adsCountList) =>
//
//            import org.json4s.JsonDSL._
//            println(s"area:ads:top3:$day , $area")
//            client.hset(s"area:ads:top3:$day", area, JsonMethods.compact(JsonMethods.render(adsCountList)))
//        }
        /*-----------------------方法一 结束---------------------------*/

        /*-----------------------方法二 开始---------------------------*/

        import org.json4s.JsonDSL._

        import scala.collection.JavaConversions._

        val area2JsonMap =
          dayAreaAdsCountListTupleArr
            .map {
              case (_, area, list) => (area, JsonMethods.compact(JsonMethods.render(list)))
            }
            .toMap
        client.hmset(s"area:ads:top3:${dayAreaAdsCountListTupleArr(0)._1}", area2JsonMap)
        /*-----------------------方法二 结束---------------------------*/


        client.close()
      })

  }

}
