package com.pgman.goku.dp23.adapter.interfaceadapter;

public class Client {
	public static void main(String[] args) {

		// 适配器类
		AbsAdapter absAdapter = new AbsAdapter() {
			// 只需要覆盖需要的方法
			@Override
			public void m1() {
				// TODO Auto-generated method stub
				System.out.println("使用了m1方法");
			}
		};
		
		absAdapter.m1();
	}

}
