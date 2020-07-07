/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/5/29 13:15
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/5/29
 * @since 1.0.0
 */
package qf.sparkDemo2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object CombineByKey_Test {
    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CombineByKey_Test"))

        //        val stuList = List(
        //            "1,杨过,男,22,bj-gp-1908",
        //            "2,郭靖,男,36,bj-gp-1907",
        //            "3,黄蓉,女,38,bj-gp-1907",
        //            "4,小龙女,女,25,bj-gp-1908",
        //            "5,郭襄,女,14,bj-gp-1909",
        //            "6,张三丰,男,14,bj-gp-1909",
        //            "8,周芷若,女,2,bj-gp-1910",
        //            "7,张无忌,男,2,bj-gp-1910"
        //        )
        //
        //        val value : RDD[String] = context.parallelize(stuList,3)
        //
        //        val value1 = value.mapPartitionsWithIndex{
        //            case (par, iterator) => {
        //                iterator.map(str => {
        //                    val str1 = str.substring(str.lastIndexOf(",") + 1)
        //                    val str2 = str.substring(0, str.lastIndexOf(","))
        //                    (str1, str2)
        //                })
        //            }


        val value1: RDD[(String, Int)] = context.parallelize(Array(("a", 2), ("a", 3), ("b", 1), ("a", 4), ("b", 5)))

        val value2: RDD[(String, (Int,Int))] = value1.combineByKey(
            (_,1),
            (arr,num)=>{(arr._1+num,arr._2+1)},
            (arr1:(Int,Int),arr2:(Int,Int))=>(arr1._1+arr2._1,arr1._2+arr2._2)
        )

        value2.foreach(println)

        value2.map({
            case (str,num) =>(str,num._1/num._2.toDouble)
        }).foreach(println)

//        def createCombiner(str: String): ArrayBuffer[String] = {
//            val buffer = new ArrayBuffer[String]()
//            buffer.append(str)
//            buffer
//        }
//
//        def mergeValue(arrayBuffer: ArrayBuffer[String], string: String): ArrayBuffer[String] = {
//            arrayBuffer.append(string)
//            arrayBuffer
//        }
//
//        def mergerCombnier(arr1: ArrayBuffer[String], arr2: ArrayBuffer[String]): ArrayBuffer[String] = {
//            arr1.++:(arr2)
//        }

    }

    def createCombiner (num:Int) : (Int,Int) = {
        (num , 1)
    }
    def mergeValue (arr:(Int,Int) , num : Int) : (Int,Int) = {
        (arr._1+num , arr._2+1)
    }
    def mergerCombnier (arr1:(Int,Int) , arr2:(Int,Int)) : (Int,Int) = {
        (arr1._1+arr2._1 , arr1._2+arr2._2)
    }
}
