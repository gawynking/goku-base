package com.pgman.goku.dp23.proxy.cglib;

public class Client {

    public static void main(String[] args) {

        TeacherDao target = new TeacherDao();

        TeacherDao proxyInstance = (TeacherDao) new ProxyFactory(target).getProxyInstance();

        proxyInstance.teach();

    }

}
