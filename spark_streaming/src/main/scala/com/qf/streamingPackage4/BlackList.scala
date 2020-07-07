/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: BlackList
 * Author: yanglan88
 * Date: 2020/6/15 14:40
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
import com.qf.SparkUtils.Spark_utils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * 在线黑名单过滤
 *  需求：
 *     从用户请求的nginx日志中过滤出黑名单的数据，保留白名单数据进行后续业务统计。
 *  data structure
 *  27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/faq.gif HTTP/1.1##200##1127
 *  110.52.250.126##2016-05-30 17:38:20##GET /data/cache/style_1_widthauto.css?y7a HTTP/1.1##200##1292
 */
object BlackList extends Logger_Trait{
    def main(args: Array[String]): Unit = {
        //1. 入口类
        val ssc = Spark_utils.getLocalStreamingContext("BlackList",2)

        //2. 加载数据
        //2.1 黑名单
        val blackListRDD:RDD[(String,Boolean)] = ssc.sparkContext.parallelize(List(
            ("27.19.74.142", true),
            ("110.52.250.126", true)
        ))
        //2.2 插入外部数据
        val lines:DStream[String] = ssc.socketTextStream("hbase1", 9999)

        //3. 黑名单过滤
        //3.1 把原始日志整理成元组
        val ip2OtherDStream = lines.map(line => {
            val index = line.indexOf("##")
            val ip = line.substring(0, index)
            val other = line.substring(index + 2)
            (ip, other)
        })

        //3.2 过滤
        val filterDStream = ip2OtherDStream.transform(rdd => {
            val join = rdd.leftOuterJoin(blackListRDD)
            join.filter { // 过滤掉所有右边为None的数据
                case (ip, (left, right)) => !right.isDefined
            }.map {
                case (ip, (left, right)) => (ip, left)
            }
        })

        //4. 打印
        filterDStream.print()
        //5. 启动
        ssc.start()
        ssc.awaitTermination()
    }
}