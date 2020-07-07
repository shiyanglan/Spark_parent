/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Window
 * Author: yanglan88
 * Date: 2020/6/15 15:57
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/15
 * @since 1.0.0
 */
package com.qf.streamingPackage4

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.Spark_utils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * window窗口操作，每个多长(M)时间，同多久时间(N)内产生数据
 * M : 滑动长度
 * N : 窗口长度
 */
object Window extends Logger_Trait{
    def main(args: Array[String]): Unit = {
        //1. 入口类
        val ssc = Spark_utils.getLocalStreamingContext("Window",2)
        //2. 读取数据
        val lines:DStream[String] = ssc.socketTextStream("hbase1", 9999)
        //3.
        val pairs:DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1))
        //4. window : reduceByKey,window
        val batchInterval = 2
        val ret = pairs.reduceByKeyAndWindow(_+_,
            windowDuration = Seconds(batchInterval * 3),
            slideDuration = Seconds(batchInterval * 2)
        )

        ret.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
