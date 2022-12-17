package com.pgman.goku.generic.test;

import com.pgman.goku.generic.tuple.Tuple2;
import com.pgman.goku.reflection.mapper.Person;

/**
 * 用于测试泛型类
 */
public class GenericClassTest {

    public static void main(String[] args) {

        // 1 Integer 类型内容
        Tuple2<Integer, Integer> integerTuple2 = new Tuple2<Integer, Integer>(1,2);
        System.out.println(integerTuple2.getName()+"-->"+ integerTuple2.getValue());

        // 2 String 类型内容
        Tuple2<String, String> stringTuple2 = new Tuple2<>("龙珠", "孙悟空");
        System.out.println(stringTuple2.getName()+"-->"+ stringTuple2.getValue());

        // 3 引用类型
        Tuple2<Person, Person> personTuple2 = new Tuple2<>(new Person("杨过", 16), new Person("小龙女", 20));
        System.out.println(personTuple2.getName()+"-->"+ personTuple2.getValue());

    }

}
