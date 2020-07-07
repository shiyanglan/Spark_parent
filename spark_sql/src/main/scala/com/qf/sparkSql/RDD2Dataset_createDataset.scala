/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo7
 * Author: yanglan88
 * Date: 2020/6/3 13:41
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
import org.apache.spark.sql.{Dataset, SparkSession}

object RDD2Dataset_createDataset extends Logger_Trait {
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("RDD2Dataset_createDataset")
        rdd2dataSet(spark)
    }

    def rdd2dataSet(spark:SparkSession) : Dataset[caseClassStu] = {
        val stuRDD :RDD[caseClassStu] = spark.sparkContext.parallelize(List(
            caseClassStu(1, "jack", "male", 22),
            caseClassStu(2, "jac", "male", 23),
            new caseClassStu(3, "jak", "male", 24)
        ))

        import spark.implicits._
        val dataFrame = stuRDD.toDF()
        val ds:Dataset[caseClassStu] = spark.createDataset[caseClassStu](stuRDD)

//        ds.show()
        ds
    }
}
case class caseClassStu(id:Int , name:String , gender:String , age:Int)
