package com.ggc.index_calc.practice2

import java.text.DecimalFormat

case class CityRemark(cityName: String, cityRatio: Double) {
  val formatter = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}
