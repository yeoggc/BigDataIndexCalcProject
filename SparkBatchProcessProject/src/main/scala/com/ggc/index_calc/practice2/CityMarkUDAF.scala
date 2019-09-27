package com.ggc.index_calc.practice2
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
class CityMarkUDAF extends UserDefinedAggregateFunction {

  // 输入数据的类型:  北京
  override def inputSchema: StructType =
    StructType(StructField("city_name", StringType) :: Nil)

  //缓存的数据的类型: 北京->1000, 天津->5000 ,  总的点击量  1000
  override def bufferSchema: StructType =
    StructType(StructField("city2countMap", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)

  //最终输出数据的类型 "北京21.2%，天津13.2%，其他65.6%"
  override def dataType: DataType = StringType

  //相同输入是否总有相同的输出
  override def deterministic: Boolean = true

  //给buffer缓存初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //初始化Map,北京->1000,天津->200 ...
    buffer(0) = Map[String, Long]()
    // 初始化总的点击量
    buffer(1) = 0L
  }


  // 分区内的聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val cityName = input.getString(0)
      var city2CountMap = buffer.getMap[String, Long](0)
      city2CountMap += cityName -> (city2CountMap.getOrElse(cityName, 0L) + 1L)
      buffer(0) = city2CountMap
      buffer(1) = buffer.getLong(1) + 1L //更新点击数

    }

  }

  // 分区间的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String, Long](0)
    val map2 = buffer2.getMap[String, Long](0)

    val resultMap = map2.foldLeft(map1) {
      case (map, (cityName, count)) => map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }

    buffer1(0) = resultMap
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)


  }

  // 最终的返回值  "北京21.2%，天津13.2%，其他65.6%"
  override def evaluate(buffer: Row): Any = {
    val city2CountMap = buffer.getAs[Map[String, Long]](0)
    val totalCount = buffer.getLong(1)

    var citiesRatio =
      city2CountMap
        .toList
        .sortBy(-_._2)
        .take(2)
        .map {
          case (cityName, count) => CityRemark(cityName, count.toDouble / totalCount)
        }

    // 如果城市的个数超过2才显示其他
    if (city2CountMap.size > 2) {
      citiesRatio = citiesRatio :+ CityRemark("其它", citiesRatio.foldLeft(1D)(_ - _.cityRatio))
    }

    citiesRatio.mkString(",")

  }
}


