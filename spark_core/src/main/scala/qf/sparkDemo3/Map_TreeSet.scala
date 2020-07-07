/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/1 19:59
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/1
 * @since 1.0.0
 */
package qf.sparkDemo3

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Map_TreeSet {
    def main(args: Array[String]): Unit = {

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Map_TreeSet"))

        val fileRDD = sc.textFile("/Users/shiyanglan/Desktop/uuu/pd.txt")

        val groupByRDD = fileRDD.map(line => {
            (line.substring(0, line.indexOf(",")), line.substring(line.indexOf(",") + 1))
        }).groupByKey()//.foreach(println)

        groupByRDD.sortBy(
            t=>t._2.map(str=>{
                str.substring(str.indexOf(",")+1).toDouble
            }),
            false,
            1
        )//.foreach(println)

//        groupByRDD.map(t => {
//            val set = mutable.TreeSet[String]()(new Ordering[String](){
//                override def compare(x: String, y: String): Int = {
//                    var i = -x.substring(x.indexOf(",") + 1).toDouble.compareTo(y.substring(y.indexOf(",") + 1).toDouble)
//                    if (i == 0) i = -x.substring(0,x.indexOf(",")).compareTo(y.substring(0,y.indexOf(",")))
//                    i
//                }
//            })
//
//            for (i <- t._2) {
//                set.add(i)
//            }
//
//            (t._1,set.take(3))
//        }).foreach(println)
    }
}
