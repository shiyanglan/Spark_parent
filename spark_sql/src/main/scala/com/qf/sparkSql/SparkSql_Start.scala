/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo1_QuickStart
 * Author: yanglan88
 * Date: 2020/6/2 15:58
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/2
 * @since 1.0.0
 */
package com.qf.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql_Start {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        //入口
        val spark = SparkSession
            .builder()
            .appName("SparkSql_Start")
            .master("local[*]")
//            .enableHiveSupport() //hive支持
            .getOrCreate() //得到Session内存有则用，无则建


        //加载数据
        val pdf : DataFrame = spark.read.json("file:///Users/shiyanglan/Desktop/spark/spark-sql/day12/people.json")


        pdf.printSchema() //打印表

        //查询数据 select * from people;
        pdf.show()

        //select name,age from people
        val df: DataFrame = pdf.select("name", "age")//.show()

        import spark.implicits._
        pdf.select($"name",$"age" + 10).show

        pdf.select($"name",($"age" + 10).as("age")).show

        pdf.select($"age").groupBy($"age").count().as("count").show

        pdf.select("name","age","height").where($"age" > 18).show()

        pdf.createOrReplaceGlobalTempView("people") //加前缀 global_temp
//        pdf.createOrReplaceTempView("people")
        spark.sql(
            """
              |select
              |age,
              |count(1)
              |from global_temp.people
              |group by age
              |""".stripMargin
        ).show()

        spark.stop()
    }
}
