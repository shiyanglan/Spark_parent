/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: KafkaManagerImpl
 * Author: yanglan88
 * Date: 2020/6/15 09:48
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/15
 * @since 1.0.0
 */
package com.qf.utils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.{JavaConversions, mutable}

object KafkaManagerImpl extends KafkaManager {

    // zookeeper的客户端
    val client = {
        val client = CuratorFrameworkFactory.builder()
            .connectString("hadoop001,hadoop002,hadoop003")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .namespace("kafka/consumers/offsets")
            .build()
        client.start()
        client
    }

    override def createMsg(ssc:StreamingContext, kafkaParams:Map[String, String], topics:Set[String]):InputDStream[(String, String)] = {
        //1. 从zookeeper中读取offset信息
        val fromOffsets:Map[TopicAndPartition, Long] = getFromOffsets(topics, kafkaParams("group.id"))
        //2. 读取外部数据
        var messages:InputDStream[(String, String)] = null
        //2.1 判断
        if (fromOffsets.isEmpty) { // 如果没有读取到偏移量，说明之前从来没有保存过，从开始的位置开始读取
            messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        }else { //读取到了偏移量，从指定位置开始读取
            //2.2 创建messageHandler
            val messageHandler = (msgHandler:MessageAndMetadata[String, String]) => (msgHandler.key(), msgHandler.message())
            //2.3 读取指定位置的offset的数据
            messages = KafkaUtils
                .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
                    ssc, kafkaParams, fromOffsets, messageHandler)
        }
        messages
    }

    override def getFromOffsets(topics:Set[String], group:String) : Map[TopicAndPartition, Long] = {
        //1. 定义一个结构专门保存偏移量
        val offsets = mutable.Map[TopicAndPartition, Long]()
        //2. 遍历主题
        for(topic <- topics) {
            //2.1 自定义offset在zookeeper的位置
            val path = s"${topic}/${group}"
            //2.2 判断zookeeper中此path路径是否存在
            isExists(path)
            //2.3 遍历获取分区:还需要将java的数组转换位scala的数组
            for(partition <- JavaConversions.asScalaBuffer(client.getChildren.forPath(path))) {
                //2.3.1 这个路径是用来保存偏移量
                val fullPath = s"${path}/${partition}"
                //2.3.2 获取偏移量
                val offset = new String(client.getData.forPath(fullPath)).toLong
                //2.3.3 数据保存offsets
                offsets.put(TopicAndPartition(topic, partition.toInt), offset)
            }
        }
        offsets.toMap
    }

    override def storeOffsets(offsetRanges:Array[OffsetRange], group:String) = {
        //1. 遍历偏移量范围的数组
        for(offsetRange <- offsetRanges) {
            //2. 获取主题分区以及偏移量
            val topic = offsetRange.topic
            val partition = offsetRange.partition
            val untilOffset = offsetRange.untilOffset
            //3. 创建保存在zookeeper上的目录
            val path = s"${topic}/${group}/${partition}"
            isExists(path)
            //4. 保存偏移量到partition
            client.setData().forPath(path, untilOffset.toString.getBytes())
        }
    }

    def isExists(path:String):Unit = {
        if (client.checkExists().forPath(path) == null) { // 如果路径不存在
            client.create().creatingParentsIfNeeded().forPath(path)
        }
    }
}
