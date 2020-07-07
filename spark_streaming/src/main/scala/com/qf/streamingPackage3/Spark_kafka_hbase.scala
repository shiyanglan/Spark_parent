/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Spark_kafka_hbase
 * Author: yanglan88
 * Date: 2020/7/5 18:02
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/7/5
 * @since 1.0.0
 */
package com.qf.streamingPackage3

import java.{lang, util}

import com.qf.SparkUtils.{CommonUtils, Spark_utils}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.{JavaConversions, mutable}
import com.qf.bigdata.HbaseUtils.HbaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes


/**
 * 一 使用hbase来手动管理offset信息，保证数据被依次消费
 * 1. 有：从指定的offset位置开始消费
 * 2. 没有：从offset为0开始消费
 *
 * 二 使用指定的offset向kafka拉取数据
 * 三 拉取到数据之后进行业务处理
 * 四 offset需要重新更新到hbase
 *
 * create 'spark-topic-offset', 'cf'
 *
 * rowkey:topic-group
 * column:partition:offset
 * */
object Spark_kafka_hbase {

    def getFromOffsets(topics: Set[String], group: String): Map[TopicAndPartition, Long] = {

        //1. 定义一个结构专门保存偏移量
        val offsets = mutable.Map[TopicAndPartition, Long]()

        //1.1 获取到HBase connection
        val connection: Connection = HbaseUtil.getConnection()
        val tableName: TableName = TableName.valueOf("spark-topic-offset")
        val cf: Array[Byte] = Bytes.toBytes("cf")

        for(topic <- topics) {
            //2.1 自定义rowkey
            val rk = s"${topic}-${group}".getBytes()

            //2.2 获取表的分区以及对应的偏移量
            val partition2Offsets: util.Map[Integer, lang.Long] = HbaseUtil.getColValue(connection, tableName, rk, cf)
            val partition2Offsets2: mutable.Map[Integer, lang.Long] = JavaConversions.mapAsScalaMap(partition2Offsets)

            //2.3 遍历获取分区:还需要将java的数组转换为scala的数组
            for ((k, v) <- partition2Offsets2) {
                offsets.put(TopicAndPartition(topic, (k+"").toInt), v)
            }
        }

        HbaseUtil.release(connection)
        offsets.toMap
    }

    def createMessage(ssc: StreamingContext, map: Map[String, String], topics: Set[String]): InputDStream[(String, String)] = {
        val offset:Map[TopicAndPartition,Long] = getFromOffsets(topics,map("group.id"))
        var message :InputDStream[(String,String)] = null

        if (offset.isEmpty){
            message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,map,topics)
        }else{
            val messageHandler = (msghandler:MessageAndMetadata[String,String])=>(
                msghandler.key(),msghandler.message())

            message = KafkaUtils
                .createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
                    ssc,map,offset,messageHandler)
        }
        message
    }

    def saveOffset(offsetRanges: HasOffsetRanges, group: String): Unit = {

        val conn: Connection = HbaseUtil.getConnection
        val tableName: TableName = TableName.valueOf("spark-topic-offset")
        val cf = Bytes.toBytes("cf")

        for (offset <- offsetRanges.offsetRanges) {

            //2. 获取主题分区以及偏移量
            val rk: Array[Byte] = s"${offset.topic}-${group}".getBytes()
            val partition: Int = offset.partition
            val untilOffset: Long = offset.untilOffset

            //3. 将结果保存到hbase
            HbaseUtil.set(conn, tableName, rk, cf, (partition+"").getBytes(), (untilOffset+"").getBytes())

        }

    }

    def main(args: Array[String]): Unit = {

        val ssc = Spark_utils.getLocalStreamingContext("Spark_kafka_hbase", 2)
        val map = CommonUtils.toMap("Demo6.properties")
        val topics = "spark".split(",").toSet

        val message :InputDStream[(String,String)] = createMessage(ssc,map,topics)

        message.foreachRDD((rdd,bTime) => {
            val ranges = rdd.asInstanceOf[HasOffsetRanges]

            //3.1 将偏移量读取到东西打印
            println("-"*100)
            println(s"bTime = ${bTime}")
            println("#"*50 + "     " + rdd.count())
            //3.2 保存最新的偏移量到zookeeper
            saveOffset(ranges,map("group.id"))
        })

        ssc.start()
        ssc.awaitTermination()

    }
}
