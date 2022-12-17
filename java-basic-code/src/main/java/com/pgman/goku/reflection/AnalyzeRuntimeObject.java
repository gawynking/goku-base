package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

import java.lang.reflect.Field;

/**
 * 利用反射分析运行时对象
 */
public class AnalyzeRuntimeObject {

    public static void main(String[] args) throws Exception{

        Person person = new Person("唐僧", 18);

        Class<? extends Person> personClass = person.getClass();

        // 1 获取属性名+属性值
        Field[] fields = personClass.getDeclaredFields();
        for(Field field:fields){
            field.setAccessible(true);
            String name = field.getName();
            Object value = field.get(person);
            System.out.println(name + " --> " + value);
        }

    }

}
