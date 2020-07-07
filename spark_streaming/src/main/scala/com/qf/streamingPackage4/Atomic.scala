/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Atomic
 * Author: yanglan88
 * Date: 2020/6/15 11:02
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
import com.qf.SparkUtils.{CommonUtils, Spark_utils}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import scalikejdbc._

//TODO 原子性操作
object Atomic extends Logger_Trait{
    def main(args: Array[String]): Unit = {
        //1. 入口类
        //TODO 主题用的是下方fromOffsets从mysql中读出的主题
//        val topics = "mytopic1".split(",").toSet
        val kafkaParams = CommonUtils.toMap("demo8.properties")
        val ssc = Spark_utils.getLocalStreamingContext("Atomic",2)

        //2. 获取到数据
        //2.1 获取到jdbc4大参数
        val jdbc = CommonUtils.toMap("db.properties")
        //2.2 设置驱动
        Class.forName(jdbc("driver"))
        //2.3 设置连接池
        ConnectionPool.singleton(jdbc("url"), jdbc("user"), jdbc("password"))
        //2.4 读取偏移量(从mysql中读取数据)
        val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly {
            implicit session => sql"select `topic`, `partid`, `offset` from mytopic".map {
                    r => TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
                }.list().apply().toMap
                //TODO 转list，调list的apply方法，转map
        }

        //2.5 messageHandler
        val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

        //2.6 获取到流数据
        val messages :InputDStream[(String,String)] =
            KafkaUtils
                .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

        //2.7 写数据
        messages.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                rdd.foreachPartition(partition => {
                    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                    //TODO 分区的偏移量 ， 获取每个分区的 offset信息
                    val pOffsetRange:OffsetRange = offsetRanges(TaskContext.get.partitionId())

                    //2.7.1 scalajdbc写数据
                    DB.localTx { // 开启事务
                        implicit session =>
                            //2.7.2 向mydata表插入数据
                            partition.foreach(msg => {
                                val name = msg._2.split(",")(0)
                                val id = msg._2.split(",")(1)
                                val dataResult = sql"""insert into `mydata`(`name`,`id`) values(${name}, ${id})""".execute().apply()
                            })
//                            val i = 1 / 0 // 如果这里报错，相当于数据自动回滚
                            //2.7.3 向mytopic表插入数据：保存偏移量
                            val offsetResult = sql"""update `mytopic` set `offset` = ${pOffsetRange.untilOffset} where `topic` = ${pOffsetRange.topic} and `partid` = ${pOffsetRange.partition}"""
                                .update().apply()
                    }// 提交事务
                })
            }
        })
        //2.8 执行
        ssc.start()
        ssc.awaitTermination()
    }
}
