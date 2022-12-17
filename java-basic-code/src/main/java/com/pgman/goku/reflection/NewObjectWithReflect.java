package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

import java.lang.reflect.Constructor;

/**
 * 利用反射实例化对象方法
 */
public class NewObjectWithReflect {

    /**
     * 通过反射机制使用有参构造实例化对象
     *  - Class.newInstance():只能调用无参构造
     *  - Constructor.newInstance():可以调用无参构造和有参构造
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Class<Person> personClass = Person.class;

        Person person1 = personClass.newInstance();
        System.out.println("方式1: " + person1);

        Constructor<Person> constructor = personClass.getConstructor();
        Person person2_1 = constructor.newInstance();
        System.out.println("方式2-1: " + person2_1);

        Constructor<Person> constructor1 = personClass.getConstructor(String.class, Integer.class);
        Person person2_2 = constructor1.newInstance("孙悟空", 30);
        System.out.println("方式2-2: " + person2_2);

    }

}
