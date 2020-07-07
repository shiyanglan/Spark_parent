/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test4
 * Author: yanglan88
 * Date: 2020/6/2 09:35
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/2
 * @since 1.0.0
 */
package qf.sparkDemo4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
 * ip,命中率(hit/miss)，响应时间，请求时间，请求方法 请求url，请求协议，状态码，相应数据流量，refer(你是从哪个url访问到当前url)，useragent
 *
 * 100.79.121.48 HIT 33 [15/Feb/2017:00:00:46 +0800] "GET http://cdn.v.abc.com.cn/videojs/video.js HTTP/1.1" 200 174055 "http://www.abc.com.cn/" "Mozilla/4.0+(compatible;+MSIE+6.0;+Windows+NT+5.1;+Trident/4.0;)"
 *
 * PV:page view : 页面浏览量，页面点击率；通常衡量一个网站或者新闻频道一条新闻的指标
 * UV:unique visitor : 指访问某个站点或者点击某条新闻的不同的ip的人数
 *
 * 需求：
 * 1. 计算独立ip数
 * 1.1 从每行日志中筛选出ip
 * 1.2 对每个ip计数
 * 1.3 去重ip，获取到独立的ip
 *
 * 2. 统计每个视频独立ip数
 * 2.1 筛选视频文件，拆分(文件名，ip地址)
 * 2.2 按照文件名称分组：(文件名，[ip,ip,ip])
 * 2.3 将每个文件名ip地址去重
 *
 * 3. 统计一天中每个小时的流量(统计每天24小时中每个小时的流量)
 *     3.1 将日志中的访问时间以及请求大小两个数据提取出来(访问时间：访问大小)，去除非法的访问(404)
 * 3.2 按照访问时间分组：(访问时间，[流量，流量，。。。])
 * 3.3 将访问时间的对应的流量叠加
 */
object Test4 {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    val IPPattern = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)".r
    val videoPattern = "([0-9]+).mp4".r
    val httpSizePattern = new Regex(".*\\s(200|206|304)\\s([0-9]+)\\s.*")
    val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Test4"))

        val valueRDD = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第四题数据-cdn.txt").cache()

//        ipStatic(valueRDD)
//        ipAlone(valueRDD)

//        videoIpStatic(valueRDD)
//        videoIpAlone(valueRDD)

        flowOfHour(valueRDD)
        hourFlow(valueRDD)
    }

    //1. 计算独立ip数
    def ipStatic(valueRDD: RDD[String]) : Unit = {

        valueRDD.map(line => {
            (IPPattern.findFirstIn(line).get, 1)
        }).reduceByKey(_ + _).sortBy(_._2, false, 1).foreach(println)

//        ipNum.take(3)
    }
    def ipAlone(rdd : RDD[String]) = {

        rdd.map(line => {
            val strings = line.split("\\s+")
            (strings(0),1)
        }).reduceByKey(_+_).sortBy(_._2, false, 1).foreach(println)

    }



    //2. 统计每个视频独立ip数
    def videoIpStatic(valueRDD: RDD[String]): Unit = {

        def getFileNameAndIp(line:String) = {
            (videoPattern.findFirstIn(line).mkString,IPPattern.findFirstIn(line).mkString)
        }

        valueRDD.filter(_.matches(".*([0-9]+)\\.mp4.*"))
            .map(getFileNameAndIp(_))
            .groupByKey().map(file_ip => (file_ip._1,file_ip._2.toList.distinct.size))
            .sortBy(_._2,false,1).foreach(println)
    }
    def videoIpAlone(rdd : RDD[String]) = {

        rdd.filter(_.matches(".*([0-9]+)\\.mp4.*"))
            .map(line=>(videoPattern.findFirstIn(line).get,IPPattern.findFirstIn(line).get))
            .groupByKey().map(tuple=>(tuple._1,tuple._2.toList.distinct))
            .sortBy(_._2.size,false,1).foreach(println)
    }



    //3. 统计一天中每个小时的流量
    def flowOfHour(valueRDD: RDD[String]): Unit = {

        def isMatch(pattern: Regex,str:String) = {
            str match {
                case pattern(_*) => true
                case _=>false
            }
        }

        def getTimeAndSize(line: String) = {
            var res = ("",0L)
            try {
                val httpSizePattern(code, size) = line
                //            println(s"${code} --> ${size}")
                val timePattern(year, hour) = line
                res = (hour, size.toLong)
            }catch {
                case e : Exception => e.printStackTrace()
            }
            res
        }
//        valueRDD.foreach(getTimeAndSize(_))

        valueRDD.filter(_.matches(httpSizePattern.toString()))
            .filter(line => isMatch(timePattern,line))
            .map(line => getTimeAndSize(line))
//            .groupByKey()
//            .map(hour_size => (hour_size._1,hour_size._2.sum))
            .reduceByKey(_+_)
            .sortByKey(false,1)
            .foreach(println)
    }
    def hourFlow(rdd : RDD[String]) = {

        rdd.map(line =>{
            val strings = line.split(" ")
            val time = strings(3)
            val logType = strings(8)
            val flow = strings(9).toLong

            val strings1 = time.split(":")
            val logTime = strings1(1)
            (logTime, (logType, flow))
        }).filter(t=>t._2._1.equals("200") || t._2._1.equals("206") || t._2._1.equals("304"))
            .groupByKey()
            .map(line => {
                (line._1,line._2.map(_._2).toList.sum)
            }).sortByKey(true,1).foreach(println)
    }

}
