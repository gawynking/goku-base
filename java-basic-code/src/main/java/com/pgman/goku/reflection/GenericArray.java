package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

import java.lang.reflect.Array;

/**
 * 泛型数组
 * - 尽量用集合类代替泛型数组
 * - 如果要用对象数组，请使用Array类型进行数据操作
 */
public class GenericArray {

    public static void main(String[] args) throws Exception{

        Person[] persons = new Person[4];
        persons[0] = new Person("唐僧",20);
        persons[1] = new Person("孙悟空",30);
        persons[2] = new Person("猪八戒",50);
        persons[3] = new Person("沙和尚",50);

        Person[] newPersons = (Person[]) copyOf(persons, 8);

        newPersons[4] = new Person("曹操",35);
        newPersons[5] = new Person("刘备",36);
        newPersons[6] = new Person("孙权",15);
        newPersons[7] = new Person("司马懿",21);

        for(Person person:newPersons){
            System.out.println(person);
        }

    }


    public static Object copyOf(Object obj,int newLength){
        Object o = null;
        Class<?> objClass = obj.getClass();
        if(objClass.isArray()) {
            o = Array.newInstance(objClass.getComponentType(), newLength);
            System.arraycopy(obj,0,o,0,Array.getLength(obj));
        }

        return o;
    }

}
