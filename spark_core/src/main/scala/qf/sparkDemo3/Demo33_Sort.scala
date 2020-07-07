package qf

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object Demo33_Sort {
    def main(args: Array[String]): Unit = {
        //1. sortByKey和SortBy.
        //1. 入口
        val context = new SparkContext(new SparkConf()
            .setAppName("Demo33_Sort")
            .setMaster("local[*]")
        )

        //2. 准备数据并指定分区数
        //2. 数据
        //2.1 stu:id name gender age class
        val stuList = List(
            Student(1,"杨过",22, 180.00),
            Student(2,"郭靖",36, 170.00),
            Student(3,"黄蓉",38, 165.00),
            Student(4,"郭芙",21, 167.00),
            Student(5,"张无忌",22, 180.00),
            Student(6,"周芷若",22, 180.00)
        )

        //2.2 处理数据
        val stuRDD = context.parallelize(stuList)
        val sortedRDD = stuRDD.sortBy(
            stu => stu.height,
            true,
            1
        )(new Ordering[Double](){
            override def compare(x: Double, y: Double): Int = y.compareTo(x)
        },
            ClassTag.Double.asInstanceOf[ClassTag[Double]]
        )
        sortedRDD.foreach(println)
    }

    def sortByKeyDemo(context:SparkContext, stuList:List[Student]): Unit = {
        val stuRDD = context.parallelize(stuList)
        val height2StuRDD = stuRDD.map(stu => (stu.height, stu))
        val sortedRDD = height2StuRDD.sortByKey(false, 1)
        sortedRDD.foreach(println)
    }
}
case class Student(id:Int, name:String, age:Int, height:Double)