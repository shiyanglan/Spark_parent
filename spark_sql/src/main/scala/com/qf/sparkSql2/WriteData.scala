/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: WriteData
 * Author: yanglan88
 * Date: 2020/6/3 16:08
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

import com.qf.utils.Spark_utils
import org.apache.spark.sql.SaveMode

object WriteData {
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("WriteData")

        val df = spark.read.json("")

        /**
         * Append : 如果保存的文件已经存在，那么就将新数据追加到源文件的末尾
         * Overwrite : 覆盖，将原来的数据删除，使用新增的数据
         * ErrorIfExists : 如果文件存在就报错
         * Ignore : 忽略，如果目录存在忽略，数据不会被报错
         */
        //        df.write.format("json").mode(SaveMode.ErrorIfExists).save("file:///e:/1") // 标准写法
        //        df.write.mode(SaveMode.ErrorIfExist).json("file:///e:/1") //简写

        df.write.format("json").mode(SaveMode.Append).save("file:///")
        df.write.mode(SaveMode.ErrorIfExists).json("")

        val url = "jdbc:mysql://localhost:3306/oa"
        val table = "t_dept"
        val properties = new Properties()
        properties.setProperty("user","root")
        properties.setProperty("password","123456")
        df.write.mode(SaveMode.Ignore).jdbc(url, table, properties)


    }
}
