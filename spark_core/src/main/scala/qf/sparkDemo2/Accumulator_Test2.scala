/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Accumulator_Test2
 * Author: yanglan88
 * Date: 2020/5/31 18:22
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/5/31
 * @since 1.0.0
 */
package qf.sparkDemo2

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Accumulator_Test2 {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Accumulator_Test"))


        val rddValue = context.parallelize(List("hadoop","hive","hbase","spark"),2)

        val accumulator = new MyAccumulator

        context.register(accumulator)

        rddValue.foreach({
            case i => accumulator.add(i)
        })

        println(accumulator)
        //TODO MyAccumulator(id: 0, name: None, value: [hbase, hadoop, hive])

        context.stop()
    }
}
class MyAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

    val list = new util.ArrayList[String]()

    override def isZero: Boolean = list.isEmpty

    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new MyAccumulator()

    override def reset(): Unit = list.clear()

    override def add(v: String): Unit = {
        if ( v.contains("h") ) {
            list.add(v)
        }
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
        list.addAll(other.value)
    }

    override def value: util.ArrayList[String] = list
}