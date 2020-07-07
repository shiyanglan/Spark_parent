/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Spark_kafka_zookeeper
 * Author: yanglan88
 * Date: 2020/6/13 12:35
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/13
 * @since 1.0.0
 */
package com.qf.streamingPackage3

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.{CommonUtils, Spark_utils}
import com.qf.utils.KafkaManagerImpl
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.{JavaConversions, mutable}

object Spark_kafka_zookeeper extends Logger_Trait{

    def main(args: Array[String]): Unit = {

        val ssc = Spark_utils.getLocalStreamingContext("Spark_kafka_zookeeper", 2)

        val kafkaParams = CommonUtils.toMap("Demo6.properties")

        val topics = "spark".split(",").toSet

        val messages:InputDStream[(String,String)] = KafkaManagerImpl.createMsg(ssc,kafkaParams,topics)

        messages.foreachRDD((rdd,bTime) => {
            if (!rdd.isEmpty()){

                println(rdd.count())

                val ranges :Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                KafkaManagerImpl.storeOffsets(ranges,kafkaParams("group.id"))
            }
        })
        ssc.start()
        ssc.awaitTermination()

    }

}
