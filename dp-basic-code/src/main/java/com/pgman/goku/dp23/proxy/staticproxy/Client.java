package com.pgman.goku.dp23.proxy.staticproxy;

/**
 * 应用实例
 * 1) 定义一个接口:ITeacherDao
 * 2) 目标对象TeacherDAO实现接口ITeacherDAO
 * 3) 使用静态代理方式,就需要在代理对象TeacherDAOProxy中也实现ITeacherDAO
 * 4) 调用的时候通过调用代理对象的方法来调用目标对象.
 */
public class Client {

    public static void main(String[] args) {

        TeacherDao teacherDao = new TeacherDao();

        TeacherDaoProxy teacherDaoProxy = new TeacherDaoProxy(teacherDao);

        teacherDaoProxy.teach();

    }

}
