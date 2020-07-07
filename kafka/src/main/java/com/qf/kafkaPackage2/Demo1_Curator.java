/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo1_Curator
 * Author: yanglan88
 * Date: 2020/6/10 09:44
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/10
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/10
 * @since 1.0.0
 */
package com.qf.kafkaPackage2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

public class Demo1_Curator {
    public static void main(String[] args) throws Exception {
        //1. 获取curator客户端对象
        //1.1 第一种方式
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("hadoop001,hadoop002,hadoop003")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        //1.2 第二种方式
//        CuratorFramework client = CuratorFrameworkFactory
//                .newClient("hadoop001,hadoop002,hadoop003", new ExponentialBackoffRetry(1000, 3));

        //2. 必须要现启动客户端对象
        client.start();
        //3. CRUD
        //3.1 增加
//        String path = client.create()
//                .creatingParentsIfNeeded() // 如果是多级目录需要创建，就会递归创建多级目录
//                .withMode(CreateMode.PERSISTENT) // 节点类型
//                .forPath("/curator/one");
//        System.out.println(path);
        System.out.println("----------------------------------------");
        //3.2 查询子目录
        List<String> children = client.getChildren().forPath("/kafka");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("----------------------------------------");

        //3.3 获取数据
        byte[] bytes = client.getData().forPath("/kafka/brokers/topics/__consumer_offsets");
        System.out.println(new String(bytes));

        //3.4 删除
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath("/curator");

        //4. 释放资源
        client.close();
    }
}