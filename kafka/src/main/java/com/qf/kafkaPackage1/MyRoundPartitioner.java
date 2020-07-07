/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: MyRoundPartitioner
 * Author: yanglan88
 * Date: 2020/6/8 19:17
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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyRoundPartitioner implements Partitioner {

    //1. 定义一个原子计数器对象：这个就是帮助我们轮询的
    private AtomicInteger count = new AtomicInteger();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionCount = cluster.partitionCountForTopic(topic);
        int partition = count.getAndIncrement() % partitionCount;
        System.out.println("key = " + key + "\t partition = " + partition);
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}


