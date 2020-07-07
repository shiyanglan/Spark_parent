/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo_
 * Author: yanglan88
 * Date: 2020/6/3 09:39
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty
import scala.collection.JavaConversions

object DataFrame_JavaList_classOf {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("DataFrame_JavaList_classOf")
            .master("local[*]")
            .getOrCreate()

        val list = List(
            new Student(1,"jack","male",22),
            new Student(2,"jac","male",23),
            new Student(3,"jak","male",24)
        )

//        import scala.collection.JavaConversions._ //隐式转换
        val javaList = JavaConversions.seqAsJavaList(list) //返回java的List
                                            //javaList 必须传Java的List
        val frame : DataFrame = spark.createDataFrame(javaList, classOf[Student])

        frame.printSchema()
        frame.show()
        spark.stop()
    }
}
class Student extends Serializable {

    @BeanProperty var id : Int = _
    @BeanProperty var name : String = _
    @BeanProperty var gender : String = _
    @BeanProperty var age : Int = _

    def this (id:Int , name:String , gender:String , age:Int){
        this
        this.id = id
        this.name = name
        this.gender = gender
        this.age = age
    }
}