package com.pgman.goku.demodi;

import java.util.Date;

public class DiDemo2 {

    private String name;
    private Integer age;
    private Date birthday;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public void print(){
        System.out.println("didemo2被调用了,name:"+name+" age:"+age+" birthday:"+birthday);
    }

}
