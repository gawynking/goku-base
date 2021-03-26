package com.pgman.goku.dp23.proxy.dynamic;

/**
 * 动态代码方式实现静态代理需求
 */
public class Client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// 创建目标对象
		ITeacherDao target = new TeacherDao();
		
		// 给目标对象创建代理对象
		ITeacherDao proxyInstance = (ITeacherDao) new ProxyFactory(target).getProxyInstance();
	
		// 输出代理对象类型
		System.out.println("proxyInstance=" + proxyInstance.getClass());
		
		//ͨ 通过代理对象调用目标对象方法
		proxyInstance.teach();
		
		proxyInstance.sayHello(" tom ");

	}

}
