/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/4 13:15
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/4
 * @since 1.0.0
 */
package qf.sparkDemo4

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO 根据用户的访问的ip地址来统计所属区域并统计访问量
object Test3_replication {

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setAppName("Test3_replication").setMaster("local[*]"))

        val httpLog : RDD[String] = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第三题数据-ipsearch/http.log")
        //20090121001942884482000|125.213.96.97|www.xs8.cn|/love/21787/index.html|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)|http://www.xs8.cn/top.php|AJSTAT_ok_times=20; AJSTAT_ok_pages=5

        val ipLog = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第三题数据-ipsearch/ip.txt")
        //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

        val mapIp: Array[(String, Long, Long)] = ipLog.map(line => {
            val strings = line.split("\\|")
            (strings(6), ip2Long(strings(0)), ip2Long(strings(1)))
        }).sortBy(
            t => t._2,
            true,
            1
        ).collect()//TODO 收集到一起 成为数组

        val broad: Array[(String, Long, Long)] = context.broadcast(mapIp).value //TODO array可通过下标查找，细节

        val provinceNumRDD = httpLog.map(line => {
            val strings = line.split("\\|")
            val ip = ip2Long(strings(1))
            if (binary(broad, ip) < 0) (null, 1)
            else (broad(binary(broad, ip))._1, 1)
        }).filter(_._1 != null).reduceByKey(_ + _)//.foreach(println)

        outPutMysql(provinceNumRDD)

    }

    def ip2Long (ip : String) : Long = {
        val strings = ip.split("\\.")
        var num = 0L
        for (str <- strings){
            num = num << 8 | str.toLong
        }
        num
    }

    def binary(ipArray:Array[(String, Long, Long)], ip: Long) : Int = {

        var end :Int= ipArray.length
        var start :Int= 0

        while(start <= end){

            var mid:Int = (end+start)/2

            if (ipArray(mid)._2 < ip && ipArray(mid)._3 > ip){
                return mid
            }else if (ipArray(mid)._2 > ip){
                end = mid - 1
            }else{
                start = mid + 1
            }
        }
        -start
    }

    def outPutMysql(rdd:RDD[(String,Int)])= {

        rdd.foreachPartition(line => {
            var conn : Connection = null
            var ps : PreparedStatement = null
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db3?useUnicode=true&amp;characterEncoding=UTF-8","root","123456")
            val sql = "insert into `db3`.`location_info` (access_date,location,counts) values(?,?,?)"
            ps = conn.prepareStatement(sql)
            line.foreach(a =>{
                ps.setDate(1, new Date(System.currentTimeMillis()))
                ps.setString(2,a._1)
                ps.setInt(3,a._2)
                ps.addBatch() // 添加到批处理缓存中
            })
            //批处理
            ps.executeBatch()
            ps.close()
        })
    }

}
