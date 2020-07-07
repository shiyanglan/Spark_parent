/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SparkStreaming_Kafka_Direct
 * Author: yanglan88
 * Date: 2020/6/11 14:58
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
import com.qf.SparkUtils.{CommonUtils, Spark_utils}
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

object SparkStreaming_Kafka_Direct {
    def main(args: Array[String]): Unit = {

        val streamingContext = Spark_utils.getLocalStreamingContext("SparkStreaming_Kafka_Direct", 2)

        val map = Map[String, String](
            "metadata.broker.list" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
//            "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
            "group.id" -> "g_1908_1",
            "auto.offset.reset" -> "largest"
        )

        val topics = "spark".split(",").toSet
//        val topics = "spark".split(",").toSet


        val map1 = CommonUtils.toMap("kafka.properties")

        val messageDStream :InputDStream[(String, String)]  = KafkaUtils
            .createDirectStream[String, String, StringDecoder, StringDecoder](
            streamingContext,
            map,
            topics
        )



        messageDStream.foreachRDD((rdd,bTime) => {
            if(!rdd.isEmpty()){
                //HasOffsetRanges 可拿到整个偏移量和下标
                val offSetRDD :HasOffsetRanges= rdd.asInstanceOf[HasOffsetRanges]
                val offsetRanges = offSetRDD.offsetRanges

                for (elem <- offsetRanges) {
                    val topic = elem.topic
                    val partition = elem.partition
                    val fromOffset = elem.fromOffset
                    val untilOffset = elem.untilOffset

                    println(s"topic = ${topic}, partition = ${partition}, fromOffset = ${fromOffset}, untilOffset = ${untilOffset}")

                }
            }
        })

        streamingContext.start()
        streamingContext.awaitTermination()

    }
}
