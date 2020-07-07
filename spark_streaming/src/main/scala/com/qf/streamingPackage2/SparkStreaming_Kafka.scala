/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SparkStreaming_Kafka
 * Author: yanglan88
 * Date: 2020/6/11 10:00
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/11
 * @since 1.0.0
 */
package com.qf.streamingPackage2

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.Spark_utils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreaming_Kafka extends Logger_Trait{
    def main(args: Array[String]): Unit = {

//        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Kafka")

        val ssc = Spark_utils.getLocalStreamingContext("SparkStreaming_Kafka", 2)

        val map = Map[String, String](
            "zookeeper.connect" -> "hadoop001,hadoop002,hadoop003/kafka",
            "group.id" -> "g_1908",
            "zookeeper.connection.timeout.ms"->"10000"
        )

        val map1 = Map[String, Int](
            "hadoop" -> 3
        )

        val message : ReceiverInputDStream[(String,String)] =
            //声明范型
            KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
            ssc,
            map,
            map1,
            StorageLevel.MEMORY_AND_DISK_SER_2
        )

        message.print()

        ssc.start()

        ssc.awaitTermination()
    }
}
