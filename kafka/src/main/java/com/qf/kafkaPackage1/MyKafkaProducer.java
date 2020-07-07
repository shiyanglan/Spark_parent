/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: MyKafkaProducer
 * Author: yanglan88
 * Date: 2020/6/8 11:03
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/8
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/8
 * @since 1.0.0
 */
package com.qf.kafkaPackage1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

class MyKafkaProducer {
    public static void main(String[] args) throws IOException {
        //1. 创建生产者对象
        //1.1 设置参数
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop001:9092,hadoop002:9092,hadoop003:9092");
//        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
//        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.load(MyKafkaProducer.class.getClassLoader().getResourceAsStream("producer.properties")); // 加载文件
        //1.2 创建对象
        /*
         * K, 代表向topic中发送的每条消息的key的类型，key可以为null
         * V, 代表向topic中发送的每一条消息value的类型
         */
        Producer<Integer, String> producer = new KafkaProducer(properties);

        //2. 发送数据
        ProducerRecord<Integer, String> record = new ProducerRecord("hadoop", "李熙"); // 1条消息
        producer.send(record); // 发送消息

        //3. 释放资源
        producer.close();
    }
}

