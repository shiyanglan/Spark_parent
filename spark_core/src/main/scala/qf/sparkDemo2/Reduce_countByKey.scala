/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/5/31 10:29
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

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Reduce_countByKey {
    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Reduce_countByKey"))

        val list = List(
            ("name", "lixi"),
            ("age", "34"),
            ("sex", "man"),
            ("salary", "1"),
            ("name", "rock")
        )

        val rddValue : RDD[(String,String)] = context.parallelize(list)

        rddValue.reduce(
            (kv1:(String,String),kv2:(String,String))=> {
            (kv1._1+"-"+kv2._1,kv1._2+kv2._2)
        })

        val tuple : (String,String) = rddValue.reduce {
            case ((k1, v1), (k2, v2)) => (k1 + "-" + k2, v1 + v2)
        }
        println(tuple)

        rddValue.countByKey().foreach(println)
    }
}
