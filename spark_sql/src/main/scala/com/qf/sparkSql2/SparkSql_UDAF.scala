/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/4 20:47
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/4
 * @since 1.0.0
 */
package com.qf.sparkSql2

import com.qf.Logger.Logger_Trait
import com.qf.utils.Spark_utils
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, IntegerType, StructField, StructType}

object SparkSql_UDAF extends Logger_Trait {
    def main(args: Array[String]): Unit = {

        val spark = Spark_utils.getLocalSparkSession("SparkSql_UDAF")

        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(List(
            caseStudent(1, "李熙", 173.0, 170.0),
            caseStudent(2, "李东", 172.0, 160.0),
            caseStudent(3, "李楠", 171.1, 150.0),
            caseStudent(4, "李北", 170.0, 140.0)
        ))

        //3. 注册
        spark.udf.register("myAvg", new MyAvg) // (name:String, MyAvg:UserDefinedAggregateFunction)
        val ds = rdd.toDS()
        ds.createOrReplaceTempView("student")
        //4. 写sql
        val sql =
            """
              |select
              |avg(height) avg_height,
              |myAvg(height) myAvg_height
              |from student
              |""".stripMargin
        spark.sql(sql).show()
        //5. 释放
        spark.stop()

    }
}
case class caseStudent(id:Int, name:String, height:Double, weight:Double)

class MyAvg extends UserDefinedAggregateFunction {

    /**
     * 指定用户自定义udaf输入参数的元信息(表头，列名)
     */
    override def inputSchema: StructType = {
        StructType(List(
//            StructField("id",DataTypes.StringType,false),
            StructField("height", DataTypes.DoubleType, false)
        ))

//        new StructType().add("height",DoubleType)
    }

    /**
     * udaf自定义函数求解过程中的临时变量的数据类型
     */
    override def bufferSchema: StructType = {
        StructType(List(
            StructField("sum", DataTypes.DoubleType, false), // sum,0
            StructField("count", DataTypes.IntegerType, false) // count,1
        ))
//  TODO      ------------buffer相当于数组 此处向数组中加入两个值，下标0与下标1

//        new StructType().add("sum",DoubleType).add("count",IntegerType)
    }

    /**
     * udaf函数的返回值类型
     * 此处求avg，所以是Double
     */
    override def dataType: DataType = DataTypes.DoubleType

    /**
     * 确定性的
     * @return
     */
    override def deterministic: Boolean = true

    /**
     * 分区内的初始化操作
     * 说的直白点就是给sum和count赋初始值,一一对应
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0,0.0)
        buffer.update(1,0)

//        buffer(0) = 0D
//        buffer(1) = 0
    }

    /**
     * 分区内的更新操作
     * buffer : 临时变量:(sum,count) -----------------------------buffer相当于数组
     * input : 自定义函数调用时传入的值:[height,]
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer.update(0,buffer.getDouble(0) + input.getDouble(0))   // sum += i
        buffer.update(1,buffer.getInt(1) + 1)   // count += 1
                 //下标的含义
//        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
//        buffer(1) = buffer.getInt(1) + 1
    }

    /**
     * 分区间的合并操作
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0)) // sum += i
        buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1)) // count += 1
                    //下标的含义
//        buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
//        buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

    /**
     * udaf聚合结果的返回值
     */
    override def evaluate(buffer: Row): Any = {
        buffer.getDouble(0) / buffer.getInt(1) // sum / count
    }
}
