/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Accumulator_Test
 * Author: yanglan88
 * Date: 2020/5/31 17:55
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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Accumulator_Test {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Accumulator_Test"))

        val list = List(
            "Apache Spark achieves high performance for both batch and streaming data, " +
                "using a state-of-the-art DAG scheduler, a query optimizer, " +
                "and a physical execution engine. "
        )
        val rddValue = context.parallelize(list)
        val wordsRDD = rddValue.flatMap(_.split("\\s+"))


        val wordAccumulator = new WordAccumulator
        context.register(wordAccumulator,"acc")

        val rbkRDD = wordsRDD.map(word => {
            if(word == "a" || word == "and") wordAccumulator.add(word) // 如果key是a字符串，就累加1
            (word, 1)
        }).reduceByKey(_+_)
        rbkRDD.foreach(println)

        println("========================= 额外的统计 ===========================")
        println("a : " + wordAccumulator)

        //TODO a : WordAccumulator(id: 0, name: Some(acc), value: Map(and -> 2, a -> 3))

//        Thread.sleep(Long.MaxValue)
        context.stop()

    }
}
class WordAccumulator extends AccumulatorV2[String, Map[String, Long]]{

    private var map = mutable.Map[String, Long]()

    override def isZero: Boolean = map.isEmpty //true

    override def copy(): AccumulatorV2[String, Map[String, Long]] = {
        val myAccumulator = new WordAccumulator
        myAccumulator.map = this.map
        myAccumulator
    }

    override def reset(): Unit = map.clear()

    override def add(word: String): Unit = {
        if (map.contains(word)) {
//            val newCount = map(word) + 1
            map.put(word, map(word) + 1)
        } else {
            map.put(word, 1)
        }
        //map.put(word, map.getOrElse())
    }

    override def merge(other: AccumulatorV2[String, Map[String, Long]]): Unit = {
        other.value.foreach {  //其他分区里的值，指的 V：Map[String, Long]
            case (word, count) => {
                if (map.contains(word)) {
                    val newCount = map(word) + count
                    map.put(word, newCount)
                } else {
                    map.put(word, count)
                }
            }
        }
    }

    override def value: Map[String, Long] = map.toMap
}