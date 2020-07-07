/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/5 18:12
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/5
 * @since 1.0.0
 */
package com.qf.sparkSql3

import com.qf.Logger.Logger_Trait
import org.apache.spark.sql.SparkSession

object DataAskew extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("DataAskew").master("local[*]").getOrCreate()

        val rdd = spark.sparkContext.parallelize(List(
            "fasfd,dgs,fasfd,rghst",
            "asf,yth,asf,ertr,hsg,dgs"
        ),4)
        //TODO 随机数范围就是分区数

        import spark.implicits._
        val df = rdd.toDF("line")

        df.createOrReplaceTempView("test")

        spark.sql(
            """
              |select
              |tmp.line line
              |from test
              |lateral view explode(split(line,",")) tmp as line
              |""".stripMargin).show()

        println("="*50)

        spark.sql(
            """
              |select
              |concat(cast(floor(rand() * 4) as String),"-",tmp.line) line
              |from
              |(select
              |tmp.line line
              |from test
              |lateral view explode(split(line,",")) tmp as line ) tmp
              |""".stripMargin).show()

        println("="*50)

        spark.sql(
            """
              |select
              |concat(cast(floor(rand() * 4) as String),"-",tmp.line) str,
              |count(1) num
              |from
              |(select
              |tmp.line line
              |from test
              |lateral view explode(split(line,",")) tmp as line ) tmp
              |group by str
              |""".stripMargin).show()

        println("="*50)

        spark.sql(
            """
              |select
              |substr(tmp1.str,instr(tmp1.str,"-") + 1) newStr,
              |sum(tmp1.num) newNum
              |from
              |(select
              |concat(cast(floor(rand() * 4) as String),"-",tmp.line) str,
              |count(1) num
              |from
              |(select
              |tmp.line line
              |from test
              |lateral view explode(split(line,",")) tmp as line ) tmp
              |group by str ) tmp1
              |group by newStr
              |""".stripMargin).show()

        spark.stop()

    }
}
