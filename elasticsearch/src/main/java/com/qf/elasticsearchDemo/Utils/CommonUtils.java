/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: CommonUtils
 * Author: yanglan88
 * Date: 2020/6/21 14:59
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
package com.qf.elasticsearchDemo.Utils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class CommonUtils {
    public static <T> Map<String,Object> bean2Map(T t) {

        Map<String,Object> map = new HashMap<>();

        try {
            Class<?> tClass = t.getClass();
            Field[] fields = tClass.getDeclaredFields();


            PropertyDescriptor pd = null;

            for (Field field : fields) {
                field.setAccessible(true);

                String name = field.getName();
//                Object o = field.get(tClass);

                pd = new PropertyDescriptor(name, tClass);
                Method method = pd.getReadMethod();
                Object invoke = method.invoke(t);

                map.put(name, invoke);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return map;
    }
}

