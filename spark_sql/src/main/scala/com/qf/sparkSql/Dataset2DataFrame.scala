/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo10
 * Author: yanglan88
 * Date: 2020/6/3 14:19
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

object Dataset2DataFrame extends Logger_Trait{
    def main(args: Array[String]): Unit = {
        val spark = Spark_utils.getLocalSparkSession("Dataset2DataFrame")

        val ds = RDD2Dataset_createDataset.rdd2dataSet(spark)

        ds2df(ds)
    }

    def ds2df(ds:Dataset[caseClassStu]) = {

        val dataFrame = ds.toDF()
        dataFrame.show()
    }
}
