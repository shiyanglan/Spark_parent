/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SparkSql_Function
 * Author: yanglan88
 * Date: 2020/6/4 10:55
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

import java.util.Properties

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils
import org.apache.spark.sql.DataFrame

object SparkSql_readjdbc extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("SparkSql_readjdbc")

        val url = "jdbc:mysql://localhost:3306/db3"
        val table = "user"
        val properties = new Properties()
        properties.setProperty("user","root")
        properties.setProperty("password","123456")
        val frame: DataFrame = spark.read.jdbc(url, table, properties)

        frame.createOrReplaceTempView("user")

        spark.sql(
            """
              |select * from user
              |""".stripMargin).show()
    }
}
