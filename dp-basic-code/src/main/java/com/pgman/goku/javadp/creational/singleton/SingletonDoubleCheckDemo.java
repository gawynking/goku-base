package com.pgman.goku.javadp.creational.singleton;

/**
 * 双重检查实现单例模式
 */
public class SingletonDoubleCheckDemo {

    private static SingletonDoubleCheckDemo instance;

    public static SingletonDoubleCheckDemo getInstance(){
        if(null == instance){
            synchronized (SingletonDoubleCheckDemo.class){
                if(null == instance){
                    instance = new SingletonDoubleCheckDemo();
                }
            }
        }
        return instance;
    }

}
