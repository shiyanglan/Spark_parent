/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TestJedis
 * Author: yanglan88
 * Date: 2020/6/17 16:29
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

import java.util.Set;

public class TestJedis {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("hadoop001",6379);
        System.out.println(jedis.ping());

        jedis.set("k1","v1");
        jedis.sadd("k2","1","2","3","4");
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            System.out.println(key);
        }
        jedis.quit();
    }
}

