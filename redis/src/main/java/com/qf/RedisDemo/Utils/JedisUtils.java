/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: JedisUtils
 * Author: yanglan88
 * Date: 2020/6/17 16:43
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
package com.qf.RedisDemo.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtils {
    private static volatile JedisPool jedisPool;

    private JedisUtils(){}

    public static JedisPool getJedisPool(){
        if (jedisPool == null) {

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxWait(1000 * 300);
            config.setMaxActive(1000);
            config.setMaxIdle(36);

            jedisPool = new JedisPool(config,"hadoop001",6379);

        }
        return jedisPool;
    }

    public static void release(JedisPool pool , Jedis jedis){
        if (jedis != null && pool == null){

        }
    }
}

