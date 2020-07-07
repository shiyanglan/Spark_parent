/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Dynamic_program
 * Author: yanglan88
 * Date: 2020/6/3 10:00
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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrame_RDD_struct {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("DataFrame_RDD_struct")
            .master("local[*]")
            .getOrCreate()

        val rdd: RDD[Row] = spark.sparkContext.parallelize(List(
            Row (1,"jac","male",22,1000D),
            Row (2,"jck","male",23,2000D),
            Row (3,"jak","male",24,3000D)
        ))

        val schema = StructType(List(
            StructField("id", DataTypes.IntegerType, false),
            StructField("name", DataTypes.StringType, false),
            StructField("gender", DataTypes.StringType, false),
            StructField("age", DataTypes.IntegerType, false),
            StructField("salary", DataTypes.DoubleType, false)
        ))

        val df = spark.createDataFrame(rdd, schema)

        df.show()
    }
}
