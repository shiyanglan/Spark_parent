/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Student
 * Author: yanglan88
 * Date: 2020/6/3 13:29
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.sparkSql;

import java.io.Serializable;

public class Stud implements Serializable {
    int id ;
    String name;
    String gender;
    int age;

    public Stud(int id,String name,String gender,int age){
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
    }
    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getGender() {
        return gender;
    }

    public int getAge() {
        return age;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

