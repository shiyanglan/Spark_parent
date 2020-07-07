/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Demo
 * Author: yanglan88
 * Date: 2020/6/10 19:40
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;

public class Demo2_CuratorServiceDiscover implements Watcher {

    private String path = "/zk";
    private CuratorFramework client;
    private List<String> children;

    public Demo2_CuratorServiceDiscover() {
        try {
            client = CuratorFrameworkFactory.builder()
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .connectString("hadoop001,hadoop002,hadoop003")
                    .build();
            client.start();
            children = client.getChildren().usingWatcher(this).forPath(path);
            System.out.println("初始监听的目录信息 ： " + children);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void start() {
        while (true){}
    }

    /**
     * 当我们监听的节点发生变化的时候，就会自动的调用这个方法
     */
    @Override
    public void process(WatchedEvent event) {
        try {
            System.out.println("当前方法被调用-----------------------------");
            List<String> newChildren = client.getChildren().usingWatcher(this).forPath(path); // 重新监听，因为这个监听在监听到一次之后就会失效
            if (newChildren.size() > children.size()) { //a b | a b c  新增节点
                for (String child : newChildren) {
                    if (!children.contains(child)) {
                        System.out.println("新增节点：" + child);
                    }
                }
            }else { // 减少
                for (String child : children) {
                    if (!newChildren.contains(child)) {
                        System.out.println("被删除的节点：" + child);
                    }
                }
            }
            // 更新数据
            children = newChildren;
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Demo2_CuratorServiceDiscover csd = new Demo2_CuratorServiceDiscover();
        csd.start();
    }
}
