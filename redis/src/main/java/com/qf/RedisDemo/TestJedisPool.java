/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TestJedisPool
 * Author: yanglan88
 * Date: 2020/6/17 16:38
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

import com.qf.RedisDemo.Utils.JedisUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestJedisPool {
    public static void main(String[] args) {

//        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxWait(1000 * 300);
//        config.setMaxActive(1000);
//        config.setMaxIdle(36);

        JedisPool jedisPool = JedisUtils.getJedisPool();

        Jedis resource = jedisPool.getResource();

    }
}

