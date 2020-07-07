/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo9
 * Author: yanglan88
 * Date: 2020/6/3 14:13
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
import org.apache.spark.sql.Dataset

object Dataset2RDD extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("Dataset2RDD")

        val ds = RDD2Dataset_createDataset.rdd2dataSet(spark)

        dataset2rdd(ds)

    }

    def dataset2rdd(ds:Dataset[caseClassStu]) = {
        val rdd = ds.rdd
        rdd.foreach(println)
    }
}
