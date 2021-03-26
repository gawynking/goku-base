package com.pgman.goku.dp23.proxy.cglib;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

public class ProxyFactory implements MethodInterceptor {

    // 聚合目标对象
    private Object target;

    public ProxyFactory(Object target) {
        this.target = target;
    }

    // 返回目标对象的一个代理对象
    public Object getProxyInstance() {

        //1. 创建工具类
        Enhancer enhancer = new Enhancer();
        //2. 设置父类
        enhancer.setSuperclass(target.getClass());
        //3. 设置回调函数
        enhancer.setCallback(this);
        //4. 创建子类对象，即代理对象
        return enhancer.create();

    }


    // 调用目标对象方法
    @Override
    public Object intercept(Object arg0, Method method, Object[] args, MethodProxy arg3) throws Throwable {

        System.out.println(" cglib proxy before ");
        Object returnVal = method.invoke(target, args);
        System.out.println(" cglib proxy after ");
        return returnVal;

    }

}
