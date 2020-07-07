package com.qf.utils

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.OffsetRange

trait KafkaManager {

    def createMsg(ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(String, String)]

    def getFromOffsets(topics: Set[String], group: String): Map[TopicAndPartition, Long]

    def storeOffsets(offsetRanges:Array[OffsetRange], group:String)

}

