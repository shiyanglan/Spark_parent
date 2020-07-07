/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SparkSql_Hive
 * Author: yanglan88
 * Date: 2020/6/3 16:28
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.sparkSql2

import java.util.Properties

import com.qf.utils.Spark_utils

object SparkSql_Hive {
    /**
     * sparksql和hive整合测试：
     * 1. 使用sparksql在hive中创建两张相关的表，并加载数据
     * 2. 完成关联查询操作，将查询的结果保存到另一张表中
     *---------------------------------
     * teacher_basic
     * name,age,married,classes
     *
     * zhangsan,23,false,0
     * lisi,24,false,0
     * wangwu,25,false,0
     * zhaoliu,26,true,1
     * zhouqi,27,true,2
     * weiba,28,true,3
     *
     * -----------------------------
     * teacher_info
     * name,height
     *
     * zhangsan,175
     * lisi,180
     * wangwu,175
     * zhaoliu,195
     * zhouqi,165
     * weiba,185
     * -------------------------------
     *
     * 完成关联查询，查询出老师的所有信息，保存在teacher表
     * create table teacher
     * as
     * select
     * b.name,
     * b.age,
     * b.married,
     * b.classes,
     * i.height
     * from teacher_basic b
     * left join
     * teacher_info i
     * on
     * b.name = i.name
     *
     *
     * sql.jar basicPath infoPath
     */
    def main(args: Array[String]): Unit = {
        //1. 校验输入参数
        if(args == null || args.length != 2) {
            println(
                """
                  |Parmeter errors! Userage : <basicPath> <infoPath>
                  |""".stripMargin)
            System.exit(-1)
        }

        //2. 将参数内容传递给数组的变量
        val Array(basicPath, infoPath) = args

        //3. 获取SparkSQL的编程入口
        val spark = Spark_utils.getLocalSparkSession("SparkSql_Hive", true)

        //4. 全程都是使用sql的方式进行编程，不能将多条sql合并到一个DataFrame执行
        //4.1 建库
        spark.sql(
            """
              |CREATE DATABASE IF NOT EXISTS `spark_sql_hive_bj1909`
              |""".stripMargin)

        spark.sql(
            """
              |USE `spark_sql_hive_bj1909`
              |""".stripMargin)

        //4.2 建表:teacher_basic
        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `spark_sql_hive_bj1909`.`teacher_basic` (
              |              name String,
              |              age int,
              |              married boolean,
              |              classes int
              |              )
              |              ROW FORMAT DELIMITED
              |FIELDS TERMINATED BY ','
              |""".stripMargin)

        //4.3 建表:teacher_info
        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `spark_sql_hive_bj1909`.`teacher_info` (
              |name String,
              |height double
              |)
              |ROW FORMAT DELIMITED
              |FIELDS TERMINATED BY ','
              |""".stripMargin)

        //4.4 加载数据:teacher_basic, teacher_info
        spark.sql(
            s"""
               |LOAD DATA INPATH '${basicPath}' INTO TABLE `spark_sql_hive_bj1909`.`teacher_basic`
               |""".stripMargin)

        spark.sql(
            s"""
               |LOAD DATA INPATH '${infoPath}' INTO TABLE `spark_sql_hive_bj1909`.`teacher_info`
               |""".stripMargin)

        //4.5 关联查询
        val joinSQL =
            """
              |CREATE TABLE IF NOT EXISTS `spark_sql_hive_bj1909`.`teacher`
              |AS
              |SELECT
              |b.name,
              |b.age,
              |b.married,
              |b.classes,
              |i.height
              |FROM
              |`spark_sql_hive_bj1909`.`teacher_basic` b
              |LEFT JOIN
              |`spark_sql_hive_bj1909`.`teacher_info` i
              |ON
              |b.name = i.name
              |""".stripMargin

        val joinDF = spark.sql(joinSQL)

        //4.6 落地数据
//        joinDF.write.saveAsTable("`spark_sql_hive_bj1909`.`teacher`")

        //5. 释放资源
        Spark_utils.stop(spark)
    }
}
