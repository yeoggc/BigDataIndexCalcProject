package com.ggc.index_calc.top10.app

import com.ggc.index_calc.top10.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp extends App {


  val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)


  val inputRDD: RDD[String] = sc.textFile("spark-index-calc/simulated_data")

  val userVisitActionRDD: RDD[UserVisitAction] = inputRDD.map(line => {
    val splits = line.split("_")
    UserVisitAction(
      splits(0), splits(1).toLong, splits(2), splits(3).toLong, splits(4), splits(5),
      splits(6).toLong, splits(7).toLong, splits(8), splits(9), splits(10), splits(11), splits(12).toLong)
  })

//  var categoryTop10Result = CategoryTop10App.statCategoryTop10_Means1(sc, userVisitActionRDD)
  //  categoryTop10Result.foreach(println)
  //  println("------------------------------")
  //  categoryTop10Result = CategoryTop10App.statCategoryTop10_Means2(sc, userVisitActionRDD)
  //  categoryTop10Result.foreach(println)
  //  println("------------------------------")
  //  categoryTop10Result = CategoryTop10App.statCategoryTop10_Means3(sc, userVisitActionRDD)
  //  categoryTop10Result.foreach(println)
  //  println("------------------------------")


//  var top10SessionBaseOnTop10CategoryRDD = Top10SessionBaseOnTop10CategoryApp.statCategoryTop10Session_Means1(sc, categoryTop10Result,userVisitActionRDD)
//  top10SessionBaseOnTop10CategoryRDD.collect.foreach(println)

//    val top10SessionBaseOnTop10CategoryRDD = Top10SessionBaseOnTop10CategoryApp.statCategoryTop10Session_Means2(sc, categoryTop10Result,userVisitActionRDD)
//    top10SessionBaseOnTop10CategoryRDD.collect.foreach(println)

//  val  top10SessionBaseOnTop10CategoryResult =  Top10SessionBaseOnTop10CategoryApp.statCategoryTop10Session_Means3(sc, categoryTop10Result,userVisitActionRDD)
//  top10SessionBaseOnTop10CategoryResult.foreach(println)

//  val  categorySessionRDD =  Top10SessionBaseOnTop10CategoryApp.statCategoryTop10Session_Means4(sc, categoryTop10Result,userVisitActionRDD)
//  categorySessionRDD.foreach(println)

  PageConversionApp.calcPageConversion(sc, userVisitActionRDD, "1,2,3,4,5,6")


  sc.stop()

}
