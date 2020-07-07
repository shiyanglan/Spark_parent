/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: ElasticSearchUtils
 * Author: yanglan88
 * Date: 2020/6/19 16:25
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/19
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/19
 * @since 1.0.0
 */
package com.qf.elasticsearchDemo.Utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class ElasticSearchUtils {

    private static TransportClient addresses = null;

    static{
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "bigdata-etc")
//                    .put("number_of_shards", 3)
//                    .put("number_of_replicas", 1)
                    .build();

            TransportClient client = new PreBuiltTransportClient(settings);
            TransportAddress[] trans = {
                    new TransportAddress(InetAddress.getByName("hadoop001"), 9300),
                    new TransportAddress(InetAddress.getByName("hadoop002"), 9300),
                    new TransportAddress(InetAddress.getByName("hadoop003"), 9300)
            };

            addresses = client.addTransportAddresses(trans);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static TransportClient getClient(){
        return addresses;
    }

}

