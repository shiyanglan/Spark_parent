/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SparkSql_UDF
 * Author: yanglan88
 * Date: 2020/6/4 15:17
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

object SparkSql_UDF extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("SparkSql_UDF")

        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(List(
            "aa",
            "lll",
            "hihao",
            "jiji"
        ))

//        spark.udf.register[Int,String]("myLen",myStrLength)

        spark.udf.register[Int,String]("myLen" , _.length)//TODO 或者用方法myStrLength

        val df = rdd.toDF("name")

        df.createOrReplaceTempView("test")

        val sql =
            """
              |select
              |name,
              |length(name) nameLen,
              |myLen(name) myNameLen
              |from
              |test
              |""".stripMargin

        spark.sql(sql).show()
    }

    def myStrLength(str:String) : Int = {
        str.length
    }
}
