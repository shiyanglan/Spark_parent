/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: LoadData
 * Author: yanglan88
 * Date: 2020/6/3 14:38
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.sparkSql2

import java.util.Properties

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils

object LoadData extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val path = "/Users/shiyanglan/Desktop/spark/spark-sql/day12/people.json"

        val spark = Spark_utils.getLocalSparkSession("LoadData")

        val dataFrame = spark.read.format("json").load("/Users/shiyanglan/Desktop/spark/spark-sql/day12/people.json")
        val dataFrame1 = spark.read.json("file:///Users/shiyanglan/Desktop/spark/spark-sql/day12/people.json")

        val dataFrame2 = spark.read.parquet(path + "")

        val dataFrame3 = spark.read.csv("").toDF("", "", "")

        val url = "jdbc:mysql://localhost:3306/oa"
        val table = "t_dept"
        val properties = new Properties()
        properties.setProperty("user","root")
        properties.setProperty("password","123456")

        spark.read.jdbc(url,table,properties)

        dataFrame.show()



        spark.stop()
    }
}
