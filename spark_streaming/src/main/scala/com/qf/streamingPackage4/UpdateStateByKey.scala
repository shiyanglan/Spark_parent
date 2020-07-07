/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: UpdateStateByKey
 * Author: yanglan88
 * Date: 2020/6/15 15:06
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

import com.qf.SparkUtils.Spark_utils
import org.apache.spark.streaming.dstream.DStream

object UpdateStateByKey {

    def main(args: Array[String]): Unit = {

        val ssc = Spark_utils.getLocalStreamingContext("UpdateStateByKey",2)

        ssc.checkpoint("file:///Users/shiyanglan/Desktop")

        val lines :DStream[String] = ssc.socketTextStream("hadoop001", 9999)

        val pairs :DStream[(String,Int)] = lines.flatMap(_.split(" ")).map((_, 1))

        val usb :DStream[(String,Int)] = pairs.updateStateByKey(updateFunc)

        usb.print()
        ssc.start()
        ssc.awaitTermination()

    }

    def updateFunc(seq:Seq[Int] , option:Option[Int]):Option[Int]={
        println(s"<option : ${option}> seq : ${seq}")
        Option(seq.sum + option.getOrElse(0))
    }
}
