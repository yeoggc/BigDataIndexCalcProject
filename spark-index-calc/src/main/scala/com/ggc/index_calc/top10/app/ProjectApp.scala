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

  //  val means1Result = CategoryTop10App.statCategoryTop10_Means1(sc, userVisitActionRDD)
  //  means1Result.foreach(println)
  //  println("------------------------------")
  //  val means2Result = CategoryTop10App.statCategoryTop10_Means2(sc, userVisitActionRDD)
  //  means2Result.foreach(println)
  //  println("------------------------------")
  val means3Result = CategoryTop10App.statCategoryTop10_Means3(sc, userVisitActionRDD)
  means3Result.foreach(println)


}
