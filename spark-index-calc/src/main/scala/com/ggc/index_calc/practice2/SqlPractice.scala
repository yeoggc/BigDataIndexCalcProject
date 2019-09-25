package com.ggc.index_calc.practice2
import org.apache.spark.sql.SparkSession

object SqlPractice extends App {

  val sparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName(getClass.getSimpleName).getOrCreate()

  sparkSession.sql("USE sql_project")
  sparkSession.udf.register("city_remark",new CityMarkUDAF)

  sparkSession
    .sql(
      """
        |SELECT
        |	ci.*,
        |	pi.product_name,
        |	uvc.click_category_id
        |FROM
        |	user_visit_action uvc JOIN product_info pi JOIN city_info ci
        |ON
        |	uvc.click_product_id = pi.product_id AND uvc.city_id = ci.city_id
        |""".stripMargin)
    .createOrReplaceTempView("t1")

  sparkSession
    .sql(
      """
        |SELECT
        |	t1.area,
        |	t1.product_name,
        |	COUNT(*) click_count,
        | city_remark(t1.city_name) city_remark
        |FROM
        |	t1
        |GROUP BY
        |	t1.area,t1.product_name
        |""".stripMargin)
    .createOrReplaceTempView("t2")

  sparkSession.sql(
    """
      |SELECT
      |	t2.*,
      |	RANK() OVER(PARTITION BY t2.area ORDER BY t2.click_count DESC) rank
      |FROM
      |	t2
      |
      |""".stripMargin)
    .createOrReplaceTempView("t3")

    sparkSession
        .sql(
          """
            |SELECT
            |	t3.area,
            |	t3.product_name,
            |	t3.click_count,
            | t3.city_remark
            |FROM
            |	t3
            |WHERE
            |	t3.rank <= 3
            |""".stripMargin)
        .show(100,truncate = false)


  sparkSession.close()


}
