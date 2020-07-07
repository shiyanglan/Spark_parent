/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/1 17:05
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

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

object TakeOrdered_Test {

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("TakeOrdered_Test"))

        val stuList = List(
            Student(1,"杨过",22, 180.00),
            Student(2,"郭靖",36, 170.00),
            Student(3,"黄蓉",38, 165.00),
            Student(4,"郭芙",21, 167.00),
            Student(5,"张无忌",23, 180.00),
            Student(6,"周芷若",21, 180.00)
        )

        val valueRDD = context.parallelize(stuList)

        valueRDD.takeOrdered(3)(new Ordering[Student](){
            override def compare(x: Student, y: Student): Int = {
                - x.d.compareTo(y.d)
            }
        }).foreach(println)
    }
}
case class Student(i: Int, str: String, i1: Int, d: Double)
