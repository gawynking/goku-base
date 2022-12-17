package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

import java.lang.reflect.Method;

/**
 * 利用反射执行方法
 */
public class ExecuteMethonWithReflect {

    public static void main(String[] args) throws Exception{

        Person person = new Person("猪八戒", 50);
        Class<? extends Person> personClass = person.getClass();

        Method getName = personClass.getMethod("getName");
        Object invoke = getName.invoke(person);
        System.out.println((String) invoke);


        Method setName = personClass.getMethod("setName",String.class);
        Object zbj = setName.invoke(person, "天蓬元帅");
        System.out.println((String) zbj);

        Object invoke1 = getName.invoke(person);
        System.out.println((String) invoke1);

    }
}
