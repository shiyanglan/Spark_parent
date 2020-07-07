/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: word_HDFS
 * Author: yanglan88
 * Date: 2020/5/26 11:33
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

object scala_wordCount {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val s = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(scala_wordCount.getClass.getSimpleName))
            .textFile("/Users/shiyanglan/Desktop/hhhhhh/hh").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
            .foreach(println)

//        Thread.sleep(100000000)

//        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("scala_wordCount"))
//
//        context.textFile("hdfs://hadoop001:9000/user_info").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).foreach(println)
//        context.stop()
    }
}
