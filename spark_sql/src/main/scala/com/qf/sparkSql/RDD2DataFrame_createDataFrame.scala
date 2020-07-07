/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo6
 * Author: yanglan88
 * Date: 2020/6/3 13:23
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
import org.apache.spark.sql.{DataFrame, SparkSession}

object RDD2DataFrame_createDataFrame extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("RDD2DataFrame_createDataFrame")

        beanRDD2DataFrame(spark)
    }

    def beanRDD2DataFrame(spark:SparkSession) : DataFrame = {

        val studRDD = spark.sparkContext.parallelize(List(
            new Stud(1, "jack", "male", 22),
            new Stud(2, "jac", "male", 23),
            new Stud(3, "jak", "male", 24)
        ))

        val dataFrame = spark.createDataFrame(studRDD, classOf[Stud])

//        dataFrame.printSchema()
//        dataFrame.show()

        dataFrame
    }
}
