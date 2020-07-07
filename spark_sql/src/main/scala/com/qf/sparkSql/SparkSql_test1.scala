/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo_test1
 * Author: yanglan88
 * Date: 2020/6/2 21:02
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/2
 * @since 1.0.0
 */
package com.qf.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

object SparkSql_test1 {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("SparkSql_test1")
            .master("local[*]")
            .getOrCreate()

        val valueRDD : RDD[String]= spark.sparkContext.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/Advert.txt")

        val value  = valueRDD.map(str => {
            val strings = str.split(" ")
            (strings(0),strings(1),strings(2),strings(3),strings(4))
        })

        import spark.implicits._
        //转换为dataFrame
        val frame : DataFrame = value.toDF("timestamp", "province", "city", "userid", "adid")

        //转换为dataSet
//        val valueDataSet : Dataset[Province_id] = frame.as[Province_id]

//        val dataFrame : DataFrame = valueDataSet.toDF()

//        val rdd : RDD[Row] = dataFrame.rdd

        frame.show()

        //3. sql
        /*
         * registerTempTable : 在spark2.0之后就已经过时了，使用createOrReplaceTempView替代

         * 创建表/视图
         * createTempView
         * createOrReplaceTempView
         *
         * createOrReplaceGlobalTempView
         * createGlobalTempView
         *
         * 从使用范围上来说：分为带global和不带的函数：
         * 1. 带global是当前spark application中可用，不带表示只能在当前的spark session中可用
         *
         * 从创建的角度说，分为带replace单词和不带的：
         * 1. 带replace的，表示创建视图，如果视图存在会覆盖之前的数据。反之，视图不存在就创建，如果视图存在就报错
         */

        frame.createOrReplaceTempView("ad")
        spark.sql(
            """
              |select
              |*
              |from
              |(select
              |province,
              |adid,
              |count(1) cnt,
              |row_number() over(distribute by province sort by count(1) desc) num
              |from ad
              |group by
              |province,adid) tmp
              |where tmp.num<=3
              |""".stripMargin).show(100)

//        (7,List((16,26), (26,25), (1,23)))
//        (5,List((14,26), (12,21), (21,21)))
//        (9,List((1,31), (28,21), (0,20)))
//        (3,List((14,28), (28,27), (22,25)))
//        (1,List((3,25), (6,23), (5,22)))
//        (8,List((2,27), (20,23), (11,22)))
//        (4,List((12,25), (16,22), (2,22)))
//        (6,List((16,23), (24,21), (27,20)))
//        (0,List((2,29), (24,25), (26,24)))
//        (2,List((6,24), (21,23), (29,20)))

//        val frame = session.read.text("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/Advert.txt")

//        frame.printSchema()
//
//        frame.show()

//        val conf = new SparkConf().setMaster("local[*]").setAppName("Demo_test1")
//        val context = new SparkContext(conf)

//        val sqlContext = new SQLContext(context)

//        val valueRDD = spark.textFile("/Users/shiyanglan/Desktop/spark/spark-core/day11/resource/Advert.txt")

//        import spark.implicits._
//        val mapDataFrame : DataFrame = valueRDD.map(str => {
//            val strings = str.split(" ")
//            Province_id(strings(0),strings(1),strings(2),strings(3),strings(4))
//        }).toDF("timestamp","province","city","userid","adid")

//        val frame1 = valueRDD.toDF("timestamp", "province", "city", "userid", "adid")


//        frame1.createOrReplaceTempView("ad")
//        session.sql(
//            """
//              |select
//              |*
//              |from ad
//              |""".stripMargin).show()

    }
}
//case class Province_id (timestamp:String , province:String , city:String , userid:String , adid:String)
