/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo
 * Author: yanglan88
 * Date: 2020/6/10 13:42
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */

/**
 * @author yanglan88
 * @create 2020/6/10
 * @since 1.0.0
 */
package com.qf.streamingPackage1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socketTextStream {
    def main(args: Array[String]): Unit = {

//        if (args == null || args.length != 2){
//            println(
//                """
//                  |Usage : <hostname> <port>
//                  |""".stripMargin)
//            System.exit(-1)
//        }

//        val Array(hostname,port) = args

        val conf = new SparkConf().setMaster("local[*]").setAppName("Demo")
        val streamingContext = new StreamingContext(conf, Seconds(2))

        //一行一行读取
        val socketLineDStream : ReceiverInputDStream[String] = streamingContext
            .socketTextStream("hadoop001", 9999)

        val word = socketLineDStream.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

        word.print()

        //启动采集器
        streamingContext.start()

        //等待采集器执行
        streamingContext.awaitTermination()
    }
}
