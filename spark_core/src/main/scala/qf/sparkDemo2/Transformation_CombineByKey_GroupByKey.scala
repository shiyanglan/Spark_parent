/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Transformation_CombineByKey_GroupByKey
 * Author: yanglan88
 * Date: 2020/5/28 21:30
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/5/28
 * @since 1.0.0
 */
package qf.sparkDemo2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Transformation_CombineByKey_GroupByKey {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Transformation_CombineByKey_GroupByKey"))

        val stuList = List(
            "1,杨过,男,22,bj-gp-1908",
            "2,郭靖,男,36,bj-gp-1907",
            "3,黄蓉,女,38,bj-gp-1907",
            "4,小龙女,女,25,bj-gp-1908",
            "5,郭襄,女,14,bj-gp-1909",
            "6,张三丰,男,14,bj-gp-1909",
            "8,周芷若,女,2,bj-gp-1910",
            "7,张无忌,男,2,bj-gp-1910"
        )

        val RDDValue : RDD[String] = context.parallelize(stuList,3)

        //匹配模式
        val value = RDDValue.mapPartitionsWithIndex {
            case (partition, iterator) => {
                iterator.map(str => {
                    val str1 = str.substring(str.lastIndexOf(",") + 1)
                    val str2 = str.substring(0, str.lastIndexOf(","))
                    (str1, str2)
                })
            }
        }

        //正常匿名函数
        RDDValue.mapPartitionsWithIndex((partition,iterable) => {
            iterable.map(line=>{
                val str1 = line.substring(line.lastIndexOf(",") + 1)
                val str2 = line.substring(0, line.lastIndexOf(","))
                (str1,str2)
            })
        })

        val value1  :RDD[(String,Iterable[String])]= value.groupByKey()
        value1.foreach(println)
        println("_"*60)


        println("====================== combineByKey ============================")
        val cbkRDD : RDD[(String,ArrayBuffer[String])]= value.combineByKey(createCombiner, mergeValue, mergeCombiner)
        cbkRDD.foreach(println)
        //5. 释放资源
        context.stop()
    }

    /**
     * key相同的会被存放到同一个ArrayBuffer
     */
    def createCombiner(stu:String):ArrayBuffer[String] = {
        println("===========================111createCombiner<"+ stu + ">=================================")
        val ab = ArrayBuffer[String]()
        ab.append(stu)
        ab
    }

    /**
     * 分区内聚合
     */
    def mergeValue(ab:ArrayBuffer[String], stu:String):ArrayBuffer[String] = {
        println("===========================222mergeValue:局部聚合<"+ ab + ">, 被聚合的值： "+ stu + " =================================")
        ab.append(stu)
        ab
    }

    /**
     * 全局聚合
     * ab1:表示全局聚合的临时结果存放处
     * ab2:某一个分区聚合的结果
     */
    def mergeCombiner(ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]):ArrayBuffer[String] = {
        println("===========================333mergeCominebier:全局聚合<"+ ab1 + ">, 局部分区聚合的值： "+ ab2 + " =================================")
        ab1.++:(ab2)

    }
}
