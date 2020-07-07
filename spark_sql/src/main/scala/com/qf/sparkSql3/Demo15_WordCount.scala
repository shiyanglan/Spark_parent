/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo15_WordCount
 * Author: yanglan88
 * Date: 2020/6/5 10:12
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/5
 * @since 1.0.0
 */
package com.qf.sparkSql3

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils

object Demo15_WordCount extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("Demo15_WordCount")

        val rdd = spark.sparkContext.parallelize(List(
            "zhang faofaks knfanfia aksjfb",
            "asdfaf,sgdh,yukiuku,rtyw"
        ))

        rdd.mapPartitionsWithIndex{
            case (par,iter) => {
                println(s"${par} - ${iter.toList.mkString(",")}")
                iter
            }
        }.foreach(println)

        import spark.implicits._
        val df = rdd.toDF("line")

        df.createOrReplaceTempView("test")

        spark.sql(
            """
              |
              |""".stripMargin).show()

    }
}
