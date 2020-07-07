/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: java_wordCount
 * Author: yanglan88
 * Date: 2020/5/26 19:02
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/5/26
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/5/26
 * @since 1.0.0
 */
package qf.spark.java_Demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class java_wordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName(java_wordCount.class.getSimpleName());

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = context.textFile("/Users/shiyanglan/Desktop/hhhhhh/hh");

        JavaRDD<String> flatMap = javaRDD.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator());

        JavaPairRDD<String, Integer> pairRDD = flatMap.mapToPair(s -> new Tuple2<String, Integer>(s, 1));

        JavaPairRDD<String, Integer> reduce = pairRDD.reduceByKey((i1, i2) -> i1 + i2);

        reduce.foreach(t -> System.out.println(t._1+"->"+t._2));


//        JavaRDD<String> stringJavaRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String s) throws Exception {
//
////                        String[] split = s.split("\\s+");
////                List<String> list = Arrays.asList(split);
////                Iterator<String> iterator = list.iterator();
////                return iterator;
//
//                return Arrays.asList(s.split("\\s+")).iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> reduce = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//
//        reduce.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> t) throws Exception {
//                System.out.println(t._1+ "->" +t._2);
//            }
//        });
//
//                stringJavaRDD.foreach(new VoidFunction<String>() {
//            public void call(String line) throws Exception {
//                System.out.println(line);
//            }
//        });
    }
}

