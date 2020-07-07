/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/1 21:03
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

object Test2_replication {
    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Test2_replication"))

        val filesRDD : RDD[String] = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第二题数据-lacduration/19735E1C66.log")

        val value : RDD[((String,String),Long)] = filesRDD.map(line => {
            val strings = line.split(",")
            val phone = strings(0)
            val time = strings(1).toLong
            val lac = strings(2)
            val logs_time = if (strings(3).equals("1")) -time else time
            ((phone, lac), logs_time)
        }).reduceByKey(_ + _)

        val value1 = value.map {
            case (tuple, time) => {
                (tuple._2, (tuple._1, time))
            }
        }

        val lacInfo = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/第二题数据-lacduration/lac_info.txt")

        val value2 = lacInfo.map(line => {
            val strings = line.split(",")
            (strings(0), (strings(1), strings(2)))
        })

        val value3 : RDD[(String,((String,Long),(String,String)))] = value1.join(value2)
        value3.foreach(println)

        val value4 : RDD[(String,(Long,(String,String)))] = value3.map(t => {
            val phone = t._2._1._1
            val time = t._2._1._2
            val xy = t._2._2
            (phone, (time, xy))
        })

        val value5 : RDD[(String,Iterable[(Long,(String,String))])] = value4.groupByKey()

        value5.mapValues(_.toList.sortWith{
            case(t1,t2) => {
                t1._1 > t2._1
            }
        }.take(2)).foreach(println)
    }
}
