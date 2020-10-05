package com.pgman.goku.demodi;

import java.util.Date;

public class DiDemo1 {

    private String name;
    private Integer age;
    private Date birthday;

    public DiDemo1(String name,Integer age,Date birthday){
        this.name = name;
        this.age = age;
        this.birthday = birthday;
    }

    public void print(){
        System.out.println("didemo1被调用了,name:"+name+" age:"+age+" birthday:"+birthday);
    }

}
