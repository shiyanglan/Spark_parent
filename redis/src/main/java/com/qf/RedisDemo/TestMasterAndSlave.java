/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TestMasterAndSlave
 * Author: yanglan88
 * Date: 2020/6/17 16:36
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/17
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/17
 * @since 1.0.0
 */
package com.qf.RedisDemo;

import redis.clients.jedis.Jedis;

public class TestMasterAndSlave {
    public static void main(String[] args) {

        Jedis master = new Jedis("hadoop001",6379);
        Jedis slave = new Jedis("hadoop001",6379);

        slave.slaveof("hadoop001",6379);

        master.set("kk","vv");

        System.out.println(slave.get("kk"));
    }
}

