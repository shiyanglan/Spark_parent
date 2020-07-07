/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Offset_Idemptent
 * Author: yanglan88
 * Date: 2020/6/15 10:04
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

import java.sql.DriverManager

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.{CommonUtils, Spark_utils}
import com.qf.utils.KafkaManagerImpl
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges

//TODO 幂等性（去重）
object Offset_Idemptent extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val topics = "test".split(",").toSet

        val ssc = Spark_utils.getLocalStreamingContext("Offset_Idemptent",3)

        val kafkaParams = CommonUtils.toMap("Demo8.properties")

        //2.获取数据
        val messages :InputDStream[(String, String)] = KafkaManagerImpl.createMsg(ssc, kafkaParams, topics)

//        保存到mysql里
//        messages.foreachRDD(rdd => {
//            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//            rdd.map(x => x._2).foreachPartition( par => {
//
//                val pOffsetRange = offsetRanges(TaskContext.get().partitionId())
//
//                val dbCon = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
//
//                par.foreach(msg =>{
//                    val id = msg.split(",")(0)
//                    val name = msg.split(",")(1)
//                    val sql = s"insert into `t_user`(`id`,`name`) value('${id}','${name}') ON DUPLICATE KEY UPDATE id = ${id}"
//                    val psmt = dbCon.prepareStatement(sql)
//                    psmt.execute()
//                })
//                dbCon.close()
//            })
//                KafkaManagerImpl.storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id"))
//        })

        messages.foreachRDD(rdd => {

            rdd.map(_._2).foreach(value => {
                val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")

                val strings = value.split(",")
                val id = strings(0)
                val name = strings(1)
                val sql = s"insert into `t_user`(`id`,`name`) value('${id}','${name}') ON DUPLICATE KEY UPDATE id = ${id}"
                val statement = connection.prepareStatement(sql)
                statement.execute()
                connection.close()
            })

        })


        ssc.start()
        ssc.awaitTermination()
    }
}
