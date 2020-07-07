/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Sql_streaming
 * Author: yanglan88
 * Date: 2020/6/15 16:09
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/15
 * @since 1.0.0
 */
package com.qf.streamingPackage4

import com.qf.SparkCommon.Logger_Trait
import com.qf.SparkUtils.Spark_utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Sql_streaming extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("Sql_streaming").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

//        ssc.checkpoint("file:///Users/shiyanglan/Desktop")

        val lines :DStream[String] = ssc.socketTextStream("hadoop001", 9999)
        //TODO 生产者输入：001 mi mobile 。。。

        val pairs :DStream[(String,Int)] =
            lines.map(line => {
            val fields = line.split("\\s+")
            if (fields == null || fields.length != 3) {
                ("", -1)
            } else {
                val brand = fields(1)
                val category = fields(2)
                (s"${category}_${brand}", 1)
            }
        }).filter(t => t._2 != -1)

        val usb:DStream[(String,Int)] = pairs.updateStateByKey(updataFunc)

        usb.foreachRDD((rdd,runtime) => {
            if (!rdd.isEmpty()){

                import spark.implicits._
                val df = rdd.map{
                    case (cb,cnt) => {
                        val category = cb.substring(0, cb.indexOf("-"))
                        val brand = cb.substring(cb.indexOf("-") + 1)
                        (category,brand,cnt)
                    }
                }.toDF("category","brand","sales")

                df.createOrReplaceTempView("test")

                val sql=
                    """
                      |select
                      |t.category,
                      |t.brand,
                      |t.sales,
                      |t.rank
                      |from
                      |(select
                      |category,
                      |brand,
                      |sales,
                      |row_number() over(partition by category order by sales desc) rank
                      |from
                      |test) t
                      |where
                      |rank < 4
                      |""".stripMargin
                spark.sql(sql).show()
            }
        })
        ssc.start()
        ssc.awaitTermination()

    }

    /**
     * seq : 表示相同的key聚合之和每一次的业务操作之后的综合
     * option : 当前那次操作的状态
     */
    def updataFunc(seq:Seq[Int] , option:Option[Int]):Option[Int]={
        Option(seq.sum + option.getOrElse(0))
    }
}
