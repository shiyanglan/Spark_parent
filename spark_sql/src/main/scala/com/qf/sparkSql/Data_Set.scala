/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Data_Set
 * Author: yanglan88
 * Date: 2020/6/3 10:15
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

object Data_Set extends Logger_Trait{

    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("Data_Set")

        val list = List(
            stu(1,"jack","male",22),
            stu(2,"jac","male",23),
            new stu(3,"jak","male",24)
        )

        import spark.implicits._
        val ds = spark.createDataset[stu](list)
        ds.printSchema()

        Spark_utils.stop(spark)
    }
}
case class stu (id:Int,name:String,gender:String,age:Int)