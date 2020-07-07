/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: MyHashPartitioner
 * Author: yanglan88
 * Date: 2020/6/8 15:06
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

public class MyHashPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer partitionCountForTopic = cluster.partitionCountForTopic(topic);

        int par = key == null ? Math.abs(value.hashCode())%partitionCountForTopic
                : (key.hashCode() & Integer.MAX_VALUE) % partitionCountForTopic;
        return par;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

