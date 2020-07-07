/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Transformation_Map
 * Author: yanglan88
 * Date: 2020/5/27 15:47
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/5/27
 * @since 1.0.0
 */
package qf.sparkDemo2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformation_Demo {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val context = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Transformation_Map"))

        //TODO map
//        val list : RDD[Int] = context.parallelize(1 to 10)
//        val rdd:RDD[Int] = list.map(_*10)
//        rdd.foreach(println) //不是连续的 默认分区了

        //TODO flatMap
//        val list = List(
//            "lijie hen shuang",    //---> String[] = {lijie, hen, shuang}
//            "hujunhuang hen shuang",
//            "shiyanglan hen shuang"
//        )
//        val value : RDD[String] = context.parallelize(list)
//        val value1 : RDD[String] = value.flatMap(_.split("\\s+"))
//        value1.foreach(println)

        //TODO filter
//        val value : RDD[Int] = context.parallelize(1 to 10)
//        val value1 :RDD[Int] = value.filter(_ % 2 == 0)
//        value1.foreach(println)






        //TODO sample
//        val list = 1 to 1000000
//
//        val listRDD:RDD[Int] = context.parallelize(list)
//
//        var retRDD:RDD[Int] = listRDD.sample(true, 0.001)
//        Thread.sleep(1000)
//        println("样本空间的元素的个数：" + retRDD.count())
//        Thread.sleep(1000)
//        retRDD = listRDD.sample(true, 0.001)
//        println("样本空间的元素的个数：" + retRDD.count())





        //TODO union distinct
        //2. 初始化rddWW
//        val listRDD1:RDD[Int] = context.parallelize(List(1, 3, 5, 7, 9))
//        val listRDD2:RDD[Int] = context.parallelize(List(2, 4, 6, 8, 10))
//        val list1UnionList2RDD:RDD[Int] = listRDD1.union(listRDD2)
//        //val list1UnionList2RDD:RDD[Int] = listRDD1++listRDD2
//
//        val distinctRDD:RDD[Int] = list1UnionList2RDD.distinct()
//
//        //3. 打印
//        list1UnionList2RDD.foreach(println)




        //TODO join
        //2. 初始化rddWW
//        2.1 数据stu:id， name, gender, age
        val stuList = List(
            "1 刘诗诗 女 18",
            "2 欧阳娜娜 女 19",
            "3 范冰冰 女 20",
            "4 大幂幂 女 21",
            "5 李冰冰 女 22"
        )

        //2.2 score:stuid, course, score
        val scoreList = List(
            "1 语文 59",
            "2 数据 60",
            "3 体育 61",
            "4 英语 62",
            "6 大数据 100"
        )
//
//        //三 加载RDD
//        //1. 加载
        val stuListRDD = context.parallelize(stuList)
        val scoreListRDD =context.parallelize(scoreList)

        //2. 处理数据变为kv
        val sid1 = stuListRDD.map(line => {
            val sid = line.substring(0, line.indexOf(" ")).toInt
            val info = line.substring(line.indexOf(" ") + 1)
            (sid, info)
        })

        val sid2 = scoreListRDD.map(line => {
            val sid = line.substring(0, line.indexOf(" ")).toInt
            val info = line.substring(line.indexOf(" ") + 1)
            (sid, info)
        })

        val value : RDD[(Int,(String,String))]= sid1.join(sid2)
        value.foreach(kv => {
            println(kv._1 + "," + kv._2._1 + "," + kv._2._2)
        })

        println("--"*30)

        val value1 : RDD[(Int,(String,Option[String]))] = sid1.leftOuterJoin(sid2)
        value1.foreach(println)

        println("--"*30)

        val value2 : RDD[(Int,(Option[String],Option[String]))] = sid1.fullOuterJoin(sid2)
        value2.foreach(println)






        //TODO groupByKey
        //2. 数据
        //2.1 stu:id name gender age class
//        val stuList = List(
//            "1,杨过,男,22,bj-gp-1908",
//            "2,郭靖,男,36,bj-gp-1907",
//            "3,黄蓉,女,38,bj-gp-1907",
//            "4,小龙女,女,25,bj-gp-1908",
//            "5,郭襄,女,14,bj-gp-1909",
//            "6,张三丰,男,14,bj-gp-1909",
//            "7,张无忌,男,2,bj-gp-1910",
//            "8,周芷若,女,2,bj-gp-1910"
//        )
//        //2.2 加载
//        val stuListRDD:RDD[String] = context.parallelize(stuList)
//        //2.3 转换数据:(bj-gp-1908, 1,杨过,男,22)
//        val class2InfoRDD:RDD[(String, String)] = stuListRDD.map(line => {
//            val dotIndex = line.lastIndexOf(",")
//            val classname = line.substring(dotIndex + 1)
//            val info = line.substring(0, dotIndex)
//            (classname, info)
//        })
//        //2.4 分组
//        val gbkRDD:RDD[(String, Iterable[String])] = class2InfoRDD.groupByKey()
//        //2.5 打印
//        gbkRDD.foreach(println)




        //TODO reduceByKey
        //2. 数据
//        val list = List(
//            "1,2,3,4,5,6",
//            "2,2,3,4,5,6",
//            "3,2,3,4,5,6"
//        )
//        val listRDD:RDD[String] = context.parallelize(list)
//        val retRDD:RDD[(String, Int)] = listRDD.flatMap(_.split(",")).map((_,1)).reduceByKey((v1,v2)=>{v1+v2})//.reduceByKey(_ + _)
//
//        //2.5 打印
//        retRDD.foreach(println)






        //TODO sortByKey
        //2. 数据
//        val list = List(
//            "1,2,3,4,5,6",
//            "2,2,3,4,5,6",
//            "3,2,3,4,5,6"
//        )
//        val listRDD:RDD[String] = context.parallelize(list)
//        val retRDD:RDD[(String, Int)] = listRDD.flatMap(_.split(",")).map((_,1)).reduceByKey(_ + _)
//        val sortRDD:RDD[(String, Int)] = retRDD.sortByKey(false) // true表示升序排序，反之就是降序
//        //2.5 打印
//        sortRDD.foreach(println) // 分区内有序，不保证全局有序




        //TODO mapPartitions
//        map是对rdd中的每一个元素进行操作；
//        mapPartitions则是对rdd中的每个分区的迭代器进行操作
//        如果是普通的map，比如一个partition中有1万条数据。ok，那么你的function要执行和计算1万次。
//        使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有
//        的partition数据。
        //2. 数据
//        val listRDD:RDD[Int] = context.parallelize(1 to 10)
//        val retRDD:RDD[Int] = listRDD.mapPartitions(_.map(_ * 2))
//        //2.5 打印
//        retRDD.foreach(println)







        //TODO cogroup
        //2. 初始化rddWW
        //2.1 数据stu:id， name, gender, age
//        val stuList = List(
//            "1 刘诗诗 女 18",
//            "1 欧阳娜娜 女 19",
//            "3 范冰冰 女 20",
//            "4 大幂幂 女 21",
//            "5 李冰冰 女 22"
//        )
//
//        //2.2 score:stuid, course, score
//        val scoreList = List(
//            "1 语文 59",
//            "1 数学 60",
//            "3 体育 61",
//            "4 英语 62",
//            "6 大数据 100"
//        )
//
//        //三 加载RDD
//        //1. 加载
//        val stuListRDD = context.parallelize(stuList)
//        val scoreListRDD =context.parallelize(scoreList)
//        //2. 处理数据变为kv
//        val sid2StuInfoRDD = stuListRDD.map(line => {
//            val sid = line.substring(0, line.indexOf(" ")).toInt
//            val info = line.substring(line.indexOf(" ") + 1)
//            (sid, info)
//        }).repartition(2)
//
//        val sid2ScoreInfoRDD = scoreListRDD.map(line => {
//            val sid = line.substring(0, line.indexOf(" ")).toInt
//            val info = line.substring(line.indexOf(" ") + 1)
//            (sid, info)
//        }).repartition(2)
//
//        //四 业务
//        val cogroupRDD:RDD[(Int, (Iterable[String], Iterable[String]))] = sid2StuInfoRDD.cogroup(sid2ScoreInfoRDD)
//        cogroupRDD.foreach {
//            case (sid, (stu, score)) => println(s"sid = ${sid}, stu = ${stu.mkString("[",",","]")}, score = ${score.mkString("[",",","]")}")
////            case _ => println("-----------")
//        }







        //TODO coalesce repartition
        //2. 数据
//        val listRDD:RDD[Int] = context.parallelize(1 to 100000)
//        println("###########listRDD分区的个数：" + listRDD.getNumPartitions)
//
//        //3. 重分区
//                var repatitionRDD = listRDD.coalesce(10,true) //默认没有开启shuffle，所以它只能做分区减少操作，要想分区增加，必须开启shuffle
//                println("###########repatitionRDD coalesce分区的个数：" + repatitionRDD.getNumPartitions)
//
//        var repatitionRDD1 = listRDD.repartition(10)
//        println("###########repatitionRDD repartition分区的个数：" + repatitionRDD1.getNumPartitions)






        //TODO mapPartitionsWithIndex
//        2. 准备数据并指定分区数
//        val listRDD = context.parallelize(1 to 10, 4) // 4个分区

//        listRDD.mapPartitionsWithIndex((num,iterator)=>{
//
//            println(s"partition = ${num} , iterator = ${iterator.mkString}")
//            iterator.map(_*2)
//
//        }).foreach(println)

//        val value : RDD[Int] = listRDD.mapPartitionsWithIndex {
//            case (partition, iterator) => println(s"partition = ${partition} , iterator = ${iterator.mkString.mkString(",")}")
//                iterator.map(_ * 2)
//        }
//        value.foreach(println)


        context.stop()
    }
}
