/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/15 21:24
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/15
 * @since 1.0.0
 */
package com.qf.streamingPackage3

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.CommonUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{JavaConversions, mutable}

object MyZookeeper extends Logger_Trait{

    val client = {
        val client = CuratorFrameworkFactory.builder()
            .connectString("hadoop001,hadoop002,hadoop003")
            .retryPolicy(new ExponentialBackoffRetry(1000,2))
            .namespace("kafka/consumers/offsets")
            .build()
        client.start()
        client
    }

    def main(args: Array[String]): Unit = {

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("MyZookeeper"))

        val ssc = new StreamingContext(sc, Seconds(3))
        val map = CommonUtils.toMap("Demo6.properties")
        val topics = "spark".split(",").toSet

        val message :InputDStream[(String,String)] = createMessage(ssc,map,topics)

        message.foreachRDD(rdd => {
            val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            storeOffset(ranges,map("group.id"))
        })
        ssc.start()
        ssc.awaitTermination()
    }

    def createMessage(ssc: StreamingContext, map: Map[String, String], topics: Set[String]): InputDStream[(String, String)] = {
        val offset:Map[TopicAndPartition,Long] = getFromOffsets(topics,map("group.id"))
        var message :InputDStream[(String,String)] = null

        if (offset.isEmpty){
            message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,map,topics)
        }else{
            val messageHandler = (msghandler:MessageAndMetadata[String,String])=>(msghandler.key(),msghandler.message())

            message = KafkaUtils
                .createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,map,offset,messageHandler)
        }
        message
    }

    def getFromOffsets(topics: Set[String], group: String): Map[TopicAndPartition, Long] = {
        val mapOffsets = mutable.Map[TopicAndPartition, Long]()

        for (topic <- topics) {
            val path = s"$topic/$group"
            pathExists(path)
            for (partition <- JavaConversions.asScalaBuffer(client.getChildren.forPath(path))){
                val fullPath = s"${path}/${partition}"
                val offsetLong = new String(client.getData.forPath(fullPath)).toLong
                mapOffsets.put(TopicAndPartition(topic,partition.toInt),offsetLong)
            }
        }

        mapOffsets.toMap
    }

    def pathExists(path: String) = {
        if (path.isEmpty){
            client.create().creatingParentsIfNeeded().forPath("path")
        }
    }

    def storeOffset(ranges: Array[OffsetRange], group: String): Unit = {
        for (offset <- ranges) {
            val topic = offset.topic
            val partition = offset.partition
            val fromOffset = offset.fromOffset
            val untilOffset = offset.untilOffset

            println(s"topic:${topic} \t partition:${partition} \t fromOffset:${fromOffset} \t untilOffset:${untilOffset}")
            val path = s"${topic}/${group}/${partition}"
            pathExists(path)
            client.setData().forPath(path,untilOffset.toString.getBytes())
        }
    }
}
