package com.pgman.goku.dp23.adapter.classadapter;

/**
 * 应用实例说明
 * 以生活中充电器的例子来讲解适配器，充电器本身相当于Adapter，220V交流电
 * 相当于src (即被适配者)，我们的目dst(即 目标)是5V直流电
 */
public class Client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(" === 类适配器 ====");
		Phone phone = new Phone();
		phone.charging(new VoltageAdapter());
	}

}
