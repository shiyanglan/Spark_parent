/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo8
 * Author: yanglan88
 * Date: 2020/6/3 13:57
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.sparkSql

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object DataFrame2RDD extends Logger_Trait{

    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("DataFrame2RDD")

        val df = RDD2DataFrame_createDataFrame.beanRDD2DataFrame(spark)

        val rdd = dataFrame2RDD(df)
//        rdd.foreach(println)
    }

    def dataFrame2RDD(df:DataFrame):RDD[Row]={
        val rdd = df.rdd
        rdd.foreach(row=>{
            val id = row.getInt(2)
            val name = row.getString(3)
            val age = row.getAs[Int]("age")
            val gender = row.getAs[String]("gender")
            println(s"${id} + ${name} + ${gender} + ${age}")
        })
        rdd
    }
}
