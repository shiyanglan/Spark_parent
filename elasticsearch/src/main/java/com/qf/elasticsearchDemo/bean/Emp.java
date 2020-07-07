/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Emp
 * Author: yanglan88
 * Date: 2020/6/21 14:55
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
package com.qf.elasticsearchDemo.bean;

public class Emp {
    private String name;
    private int age;

    public Emp(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Emp() {
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

