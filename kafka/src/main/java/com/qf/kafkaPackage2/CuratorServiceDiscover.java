/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: CuratorServiceDiscover
 * Author: yanglan88
 * Date: 2020/6/10 10:08
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

public class CuratorServiceDiscover  implements Watcher {

    private String path = "/zk";
    private List<String> children;
    private CuratorFramework client;

    public CuratorServiceDiscover(){
        try {
            client = CuratorFrameworkFactory.builder()
                    .connectString("hadoop001,hadoop002,hadoop003")
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            client.start();
            children = client.getChildren().usingWatcher(this).forPath(path);
                                        //为操作设置监视程序
            System.out.println(children);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void start(){
        while (true){
//        System.out.println("nizaigm________________________");
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        //TODO 反复调用死循环 反复new对象 不好！！！
        CuratorServiceDiscover csd = new CuratorServiceDiscover();
        System.out.println("jjjjjjjj");
        csd.start();
    }

    public static void main(String[] args) {
        CuratorServiceDiscover curatorServiceDiscover = new CuratorServiceDiscover();
        curatorServiceDiscover.start();
    }
}
