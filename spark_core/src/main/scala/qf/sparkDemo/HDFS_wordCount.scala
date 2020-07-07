/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: HDFS_wordCount
 * Author: yanglan88
 * Date: 2020/5/26 21:05
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/5/26
 * @since 1.0.0
 */
package qf.sparkDemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HDFS_wordCount {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 1) {
            println(
                """
                  |Usage:<input>
                  |""".stripMargin)
            System.exit(-1)
        }

        val Array(input) = args // 将输入参数赋值给input变量

        val context = new SparkContext(new SparkConf().setMaster("yarn").setAppName("HDFS_wordCount"))

        context.textFile(input).flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_ + _).foreach(println)

        context.stop()

    }
}