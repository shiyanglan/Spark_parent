/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test1
 * Author: yanglan88
 * Date: 2020/6/1 14:34
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
数据格式：
timestamp province city userid adid

userid:0~99
省份和城市：ID：0~9
adid：0~19

需求：
1. 统计每个省份的点击top3的广告id
2. 统计每个省份每一个小时的top3的广告id

1516609143867 6 7 64 16
 */
object Test1 {
    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Test1"))

        val logsRDD = context.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/Advert.txt")

        val logsArrRDD = logsRDD.map(_.split("\\s+"))

        val provinceAndAd2CntRDD = logsArrRDD.map(arrays => (arrays(1) + "_" + arrays(4), 1))

        val proAndAdCntRDD = provinceAndAd2CntRDD.reduceByKey(_ + _)

        val proAndAdCount2RDD = proAndAdCntRDD.map(province_adid_cnt => {
            val param = province_adid_cnt._1.split("_")
            (param(0), (param(1), province_adid_cnt._2))
        })

        val pro2AdArrayRDD: RDD[(String, Iterable[(String, Int)])] = proAndAdCount2RDD.groupByKey()
        pro2AdArrayRDD.foreach(println)

        pro2AdArrayRDD.mapValues(values => values.toList.sortWith(
            (current, next) => current._2 > next._2
        ).take(10)
        ).foreach(println)

        context.stop()
    }
}
