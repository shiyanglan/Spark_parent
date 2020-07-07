/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test2
 * Author: yanglan88
 * Date: 2020/6/1 14:59
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/1
 * @since 1.0.0
 */
package qf.sparkDemo4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据格式：
 *
 * 19735E1C66.log ： 存储的日志信息
 * 手机号码，时间戳，基站id，连接状态(1连接，0断开)
 * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
 *
 * lac_info.txt ： 存储基站信息
 * 基站id，经度，纬度
 * 9F36407EAD8829FC166F14DDE7970F68,116.304864,40.050645,6
 *
 * 需求：
 * 根据用户产生的日志信息，分析在哪个基站停留的时间最长
 * 在一定范围内，求所有用户经过的所有基站所停留时间最长的TOP2
 */
object Test2 {
    /**
     * 1. 获取用户产生的日志信息并切分
     * 2. 用户在基站停留的总时长
     * 3. 获取基站的基本信息
     * 4. 把经纬度的信息join到用户数据中
     * 5. 求除用户在某些基站停留时间的top2
     */
    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Test2"))

        val filesRDD : RDD[String] = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第二题数据-lacduration/19735E1C66.log")

        val userInfoRDD : RDD[((String,String),Long)] = filesRDD.map(line => {
            val fields : Array[String] = line.split(",")
            val phone = fields(0)
            val time = fields(1).toLong
            val lac = fields(2)
            val event_Type = fields(3)
            val time_long = if (event_Type.equals("1")) -time else time //TODO 细节，结合下述reduceByKey，获得停留时间
            //TODO 拼_ 也可以 ， 但为了后期方便
            ((phone,lac),time_long)
        })

        val sumRDD : RDD[((String,String),Long)] = userInfoRDD.reduceByKey(_ + _)

        val lacAndPT : RDD[(String,(String,Long))] = sumRDD.map(tup => {
            val phone = tup._1._1
            val lac = tup._1._2
            val time = tup._2
            (lac, (phone, time))
        })

        val lacInfo = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第二题数据-lacduration/lac_info.txt")

        val lacAndXY : RDD[(String,(String,String))] = lacInfo.map(line => {
            val fields = line.split(",")
            val lac = fields(0)
            val x = fields(1)
            val y = fields(2)
            (lac, (x, y))
        })

        val joinRDD : RDD[(String,((String,Long),(String,String)))] = lacAndPT.join(lacAndXY)

        val phoneAndTXY : RDD[(String,Long,(String,String))] = joinRDD.map(tup => {
            val phone = tup._2._1._1
            val time = tup._2._1._2
            val xy = tup._2._2
            (phone,time,xy)
        })

        val groupByRDD : RDD[(String,Iterable[(String , Long,(String,String))])] = phoneAndTXY.groupBy(_._1)

        val sorded: RDD[(String, List[(String, Long, (String, String))])] = groupByRDD.mapValues(_.toList.sortBy(_._2).reverse)

        val res: RDD[(String, List[(Long, (String, String))])] = sorded.map(tup => {
            val phone = tup._1
            val list = tup._2
            val filterList = list.map(t => {
                val time = t._2 //时间
                val xy = t._3
                (time, xy)
            })
            (phone, filterList)
        })

        res.mapValues(_.take(2)).foreach(println)
    }
}
