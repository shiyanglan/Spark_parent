/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: AggregateByKey_Test
 * Author: yanglan88
 * Date: 2020/6/26 21:43
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/26
 * @since 1.0.0
 */
package qf.sparkDemo2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object AggregateByKey_Test {
    def main(args: Array[String]): Unit = {

        //1. 入口
        val context = new SparkContext(new SparkConf()
            .setAppName("AggregateByKey_Test")
            .setMaster("local[*]")
        )

        //        abk2rbkDemo(context) // reduceByKey
        abk2gbkDemo(context) // groupByKey
    }

    // reduceByKey
    def abk2rbkDemo(context:SparkContext): Unit = {
        //2. 准备数据并指定分区数
        //2. 数据
        //2.1 stu:id name gender age class
        val stuList = List(
            "1,杨过,男,22,bj-gp-1908",
            "2,郭靖,男,36,bj-gp-1907",
            "3,黄蓉,女,38,bj-gp-1907",
            "4,小龙女,女,25,bj-gp-1908",
            "5,郭襄,女,14,bj-gp-1909",
            "6,张三丰,男,14,bj-gp-1909",
            "7,张无忌,男,2,bj-gp-1910",
            "8,周芷若,女,2,bj-gp-1910"
        )

        val stuListRDD = context.parallelize(stuList)

        val pairRDD = stuListRDD.flatMap(_.split(",")).map((_,1))
        println("====================== reduceByKey ============================")

        pairRDD.reduceByKey(_ + _).foreach(println)
        println("====================== aggregateByKey ============================")
        /*
         *  seqOp : 按相同的一组key的分区内部进行聚合
         *  combOp : 按相同的一组key的全分区进行聚合
         *  zeroValue : 初始值
         */
        val abkRDD:RDD[(String, Int)] = pairRDD.aggregateByKey(0)(_ + _, _ + _)
        abkRDD.foreach(println)
    }

    //groupByKey
    def abk2gbkDemo(context:SparkContext): Unit = {
        //2. 准备数据并指定分区数
        //2. 数据
        //2.1 stu:id name gender age class
        val stuList = List(
            "1,杨过,男,22,bj-gp-1908",
            "2,郭靖,男,36,bj-gp-1907",
            "3,黄蓉,女,38,bj-gp-1907",
            "4,小龙女,女,25,bj-gp-1908",
            "5,郭襄,女,14,bj-gp-1909",
            "6,张三丰,男,14,bj-gp-1909",
            "7,张无忌,男,2,bj-gp-1910",
            "8,周芷若,女,2,bj-gp-1910"
        )
        val stuListRDD = context.parallelize(stuList,3)

        val class2InfoRDD = stuListRDD.mapPartitionsWithIndex {
            case (partition, iterator) => {
                val array = iterator.toArray
                //                println(s"${partition} : ${array.mkString("[",",","]")}")
                array.map(line => {
                    val dotIndex = line.lastIndexOf(",")
                    val classname = line.substring(dotIndex + 1)
                    val info = line.substring(0, dotIndex)
                    (classname, info)
                }).iterator
            }
        }

        println("====================== groupByKey ============================")
        val gbkRDD = class2InfoRDD.groupByKey()
        gbkRDD.foreach(println)

        println("====================== aggregateByKey ============================")
        val abkRDD:RDD[(String, ArrayBuffer[String])] = class2InfoRDD.aggregateByKey(ArrayBuffer[String]())(seqOp, combOp)
        abkRDD.foreach(println)
    }

    //局部聚合
    def seqOp(ab:ArrayBuffer[String], info:String):ArrayBuffer[String] = {
        ab.append(info)
        ab
    }

    //全局聚合
    def combOp(ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]):ArrayBuffer[String] = {
        ab1.++:(ab2)
    }

}
