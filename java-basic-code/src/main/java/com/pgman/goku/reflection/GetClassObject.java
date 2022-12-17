package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

/**
 * 获取Class对象的三种方式
 * 还有一种可以通过类加载器进行获取实例对象的方法
 */
public class GetClassObject {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
        newObjectTypeOne();
        newObjectTypeTwo();
        newObjectTypeThree();
    }


    /**
     * 通过全限定类名获取有参Class对象
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    public static void newObjectTypeOne() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        Class<?> person = Class.forName("com.pgman.goku.reflection.mapper.Person");
        System.out.println("方式1: " + person);
    }

    /**
     * 通过ClassName.class获取Class对象
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static void newObjectTypeTwo() throws InstantiationException, IllegalAccessException {
        Class<Person> personClass = Person.class;
        System.out.println("方式1: " + personClass);
    }

    public static void newObjectTypeThree() throws InstantiationException, IllegalAccessException {
        Person person = new Person();
        Class<? extends Person> aClass = person.getClass();
        System.out.println("方式1: " + aClass);
    }

}
