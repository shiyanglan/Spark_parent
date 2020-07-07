/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/21 18:11
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/21
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/21
 * @since 1.0.0
 */
package com.qf.elasticsearchDemo;

import com.qf.elasticsearchDemo.bean.Emp;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class Test {

    private static TransportClient client;

    static {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "bigdata-etl")
                    .build();

            PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);

            TransportAddress[] addresses = {
                    new TransportAddress(InetAddress.getByName("hadoop001"), 9300),
                    new TransportAddress(InetAddress.getByName("hadoop002"), 9300),
                    new TransportAddress(InetAddress.getByName("hadoop003"), 9300)
            };

            client = preBuiltTransportClient.addTransportAddresses(addresses);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

    public void createIndex() throws IOException {

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject()
                .field("name","jjj")
                .field("age",22)
                .endObject();

        String json = "{\"name\":\"jjj\",\"age\":\"44\"}";

        Map<String, Object> bean2Map = bean2Map(new Emp("jjj", 34));

        Map<String,Object> map = new HashMap<>();
        map.put("name","jjj");
        map.put("age",22);

        IndexResponse response = client.prepareIndex("bigdata","emp","1")
                .setSource(json,XContentType.JSON)
                .setSource(map,XContentType.JSON)
                .setSource(bean2Map,XContentType.JSON)
                .setSource(builder)
                .get();
        System.out.println(response.getVersion());
    }

    public static <T> Map<String,Object> bean2Map(T t) {
        Map<String,Object> map = new HashMap<>();

        try {
            Class<?> tClass = t.getClass();
            Field[] fields = tClass.getDeclaredFields();
//            PropertyDescriptor descriptor = null;

            for (Field field : fields) {
                String name = field.getName();
                PropertyDescriptor descriptor = new PropertyDescriptor(name, tClass);
                Method method = descriptor.getReadMethod();
                Object o = method.invoke(tClass);

                map.put(name,o);
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        return map;
    }
}
