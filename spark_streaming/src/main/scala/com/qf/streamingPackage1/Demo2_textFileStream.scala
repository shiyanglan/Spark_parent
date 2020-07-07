/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: HDFS_demo
 * Author: yanglan88
 * Date: 2020/6/10 16:23
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

import com.qf.SparkCommon.Logger_Trait
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo2_textFileStream extends Logger_Trait{
    def main(args: Array[String]): Unit = {
        //1. 入口
        val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("Demo2_textFileStream"), Seconds(2))
        //2. 读取本地数据
        val lines:DStream[String] = ssc.textFileStream("/Users/shiyanglan/Desktop/hhhhhh")
//                val lines:DStream[String] = ssc.textFileStream("hdfs://hadoop001:9000/spark/data")

        //3. 打印
        lines.print()
        //4. 开启
        ssc.start()
        ssc.awaitTermination()
    }
}
