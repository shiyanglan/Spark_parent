/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/4 23:59
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/4
 * @since 1.0.0
 */
package com.qf.sparkSql2

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils
import org.apache.spark.rdd.RDD

//TODO 查询：所有订单中每年的销售单数、销售总额
//TODO 查询：所有订单每年最大金额订单的销售额
//TODO 查询：所有订单中每年最畅销货品
object Test extends Logger_Trait {
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("Test")

        val tbStockRDD: RDD[String] = spark.sparkContext.textFile("/Users/shiyanglan/Desktop/nihao/tbStock")
        val tbStockDetailRDD: RDD[String] = spark.sparkContext.textFile("/Users/shiyanglan/Desktop/nihao/tbStockDetail")
        val tbDateRDD: RDD[String] = spark.sparkContext.textFile("/Users/shiyanglan/Desktop/nihao/tbDate")

        import spark.implicits._
        val newTbStockRDD = tbStockRDD.map(str => {
            val strings = str.split(" ")
            (strings(0), strings(1), strings(2))
        })
        val tbStockDF = newTbStockRDD.toDF("orderNumber", "locationId", "dateId")

        val newTbStockDetailRDD = tbStockDetailRDD.map(str => {
            val strings = str.split(" ")
            (strings(0), strings(1), strings(2),strings(3).toInt,strings(4),strings(5).toDouble)
        })
        val tbStockDetailDF = newTbStockDetailRDD.toDF("orderNumber", "rowNum", "itemId","number","price","amount")

        val newTbDateRDD = tbDateRDD.map(str => {
            val strings = str.split(" ")
            (strings(0), strings(1), strings(2),strings(3), strings(4), strings(5),strings(6), strings(7), strings(8),strings(9))
        })
        val tbDateDF = newTbDateRDD.toDF("dateId", "years", "theYear", "month", "day", "weekday", "week", "quarter", "period", "halfMonth")

        tbStockDF.createOrReplaceTempView("tbStock")
        tbStockDetailDF.createOrReplaceTempView("tbStockDetail")
        tbDateDF.createOrReplaceTempView("tbDate")

//        spark.sql(
//            """
//              |select * from tbStockDetail
//              |""".stripMargin).show()
//
//        tbStockDetailDF.printSchema()

        //TODO 查询：所有订单中每年的销售单数、销售总额
        spark.sql(
            """
              |SELECT
              |year(st.dateId) year,
              |count(distinct(st.orderNumber)) orderNum,
              |nvl(sum(sd.amount),0) sum
              |FROM tbStock st left join tbStockDetail sd
              |ON st.orderNumber = sd.orderNumber
              |GROUP BY year
              |""".stripMargin).show()


        spark.sql("desc function distinct" ).show()  //TODO distinct是个函数


        //TODO 查询：所有订单每年最大金额订单的销售额

        spark.sql(
            """
              |SELECT
              |*
              |FROM
              |(SELECT
              |year(st.dateId) year,
              |sum(sd.amount) sum,
              |row_number() over(distribute by year(st.dateId) sort by sum(sd.amount) desc) num
              |FROM tbStock st join tbStockDetail sd
              |ON st.orderNumber = sd.orderNumber
              |GROUP BY year,sd.orderNumber) tmp
              |WHERE tmp.num=1
              |""".stripMargin).show()

        spark.sql(
            """
              |SELECT
              |tmp.year year,
              |max(tmp.sum) max
              |FROM
              |(SELECT
              |year(st.dateId) year,
              |sum(sd.amount) sum
              |FROM tbStock st join tbStockDetail sd
              |ON st.orderNumber = sd.orderNumber
              |GROUP BY year,sd.orderNumber) tmp
              |GROUP BY tmp.year
              |""".stripMargin).show()

        //TODO 查询：所有订单中每年最畅销货品

        spark.sql(
            """
              |SELECT
              |*
              |FROM
              |(SELECT
              |year(st.dateId) year,
              |sd.itemId item,
              |sum(sd.number) itemCnt,
              |rank() over(distribute by year(st.dateId) sort by sum(sd.number) desc) num
              |FROM
              |tbStock st join tbStockDetail sd
              |ON st.orderNumber = sd.orderNumber
              |GROUP BY year,sd.itemId ) tmp
              |WHERE tmp.num=1
              |""".stripMargin).show()


        spark.stop()

    }
//    def splitRDD(rdd:RDD[String]):RDD[(String*)]={
//        rdd.map(str=>{
//            str.split(" ").foreach()
//        })
//    }
}
