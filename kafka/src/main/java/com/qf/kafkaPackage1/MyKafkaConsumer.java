/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: MyKafkaConsumer
 * Author: yanglan88
 * Date: 2020/6/8 14:33
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.load(MyKafkaConsumer.class.getClassLoader().getResourceAsStream("consumer.properties"));

        Consumer<Integer,String> consumer = new KafkaConsumer(properties);

        //订阅主题
        consumer.subscribe(Arrays.asList("hadoop"));

        while(true){
            ConsumerRecords<Integer , String> record = consumer.poll(1000);

            for (ConsumerRecord<Integer, String> consumerRecord : record) {
                Integer key = consumerRecord.key();
                String value = consumerRecord.value();

                int partition = consumerRecord.partition();
                long offset = consumerRecord.offset();
                String topic = consumerRecord.topic();

                System.out.println(key +"-"+ value +"-"+ partition +"-"+ offset +"-"+ topic);
            }
        }
    }
}

