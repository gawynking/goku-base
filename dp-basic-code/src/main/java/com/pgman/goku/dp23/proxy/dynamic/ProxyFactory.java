package com.pgman.goku.dp23.proxy.dynamic;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;


public class ProxyFactory {

    // 维护目标对象 - Object
    private Object target;

    // 构造器初始化
    public ProxyFactory(Object target) {
        this.target = target;
    }


    // 核心方法 ：给目标对象生成代理对象
    public Object getProxyInstance() {

        /**
         * 说明：
         *
         * public static Object newProxyInstance(ClassLoader loader,
         *                                           Class<?>[] interfaces,
         *                                           InvocationHandler h)
         *
         *  1.ClassLoader loader ： 指定当前目标对象使用的类加载器，获取加载器的方法
         *  2.Class<?>[] interfaces ：目标对象实现的接口类型，使用泛型方法确认类型
         *  3.InvocationHandler h ：事件处理，执行目标对象的方法时，会触发事情处理器方法，会把当前执行的目标对象的方法作为参数传入
         */
        return Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                target.getClass().getInterfaces(),
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

                        System.out.println(" jdk proxy before ... ");
                        // 反射机制调用目标方法
                        Object returnVal = method.invoke(target, args);
                        System.out.println(" jdk proxy after ...");
                        return returnVal;
                    }

                });

    }

}
