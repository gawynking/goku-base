package com.pgman.goku.example.demoxml.factory;

import com.pgman.goku.example.demoxml.domain.Emp;
import com.pgman.goku.example.demoxml.service.IEmpService;
import com.pgman.goku.example.demoxml.utils.TransactionManager;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

public class BeanFactory {

    IEmpService empService;

    TransactionManager transactionManager;

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public final void setEmpService(IEmpService empService) {
        this.empService = empService;
    }

    public IEmpService getEmpService() {
        return (IEmpService)Proxy.newProxyInstance(empService.getClass().getClassLoader(),
                empService.getClass().getInterfaces(),
                new InvocationHandler() {
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

                        Object returnValue = null;
                        try {
                            //1.开启事务
                            transactionManager.beginTransaction();
                            //2.执行操作
                            returnValue = method.invoke(empService,args);
                            //3.提交事务
                            transactionManager.commit();
                            //4.返回结果
                            return returnValue;
                        }catch (Exception e){
                            //5.回滚操作
                            transactionManager.rollback();
                            throw new RuntimeException(e);
                        }finally {
                            //6.释放连接
                            transactionManager.release();
                        }

                    }
                }
        );
    }


}
