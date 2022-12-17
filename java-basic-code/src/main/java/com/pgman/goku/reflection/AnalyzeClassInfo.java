package com.pgman.goku.reflection;

import com.pgman.goku.reflection.mapper.Person;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * 利用反射分析类静态结构信息
 *  Class<?>.getXxx
 */
public class AnalyzeClassInfo {

    public static void main(String[] args) throws Exception{

        Class<Person> personClass = Person.class;

        // 1 获取属性信息
        Field id = personClass.getField("id");
        System.out.println("获取公共属性: " + id.getName());

        Field name = personClass.getDeclaredField("name");
        System.out.println("获取类属性: " + name.getName());

        Field[] fields = personClass.getFields();
        Field[] declaredFields = personClass.getDeclaredFields();

        // 2 获取方法信息
        Method setName = personClass.getMethod("setName",String.class);
        System.out.println("获取类方法: " + setName.getName() + " 参数个数 " + setName.getParameterCount());

        Method[] methods = personClass.getMethods();
        Method[] declaredMethods = personClass.getDeclaredMethods();

        // 3 获取构造器信息
        Constructor<Person> constructor = personClass.getConstructor(String.class, Integer.class);
        System.out.println("获取类构造器: " + constructor);

        Constructor<?>[] constructors = personClass.getConstructors();
        Constructor<?>[] declaredConstructors = personClass.getDeclaredConstructors();

    }
}
