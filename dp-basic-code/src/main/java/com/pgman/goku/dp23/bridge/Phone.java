package com.pgman.goku.dp23.bridge;

public abstract class Phone {
	
	// 组合品牌
	private Brand brand;

	public Phone(Brand brand) {
		super();
		this.brand = brand;
	}
	
	public void open() {
		this.brand.open();
	}
	public void close() {
		this.brand.close();
	}
	public void call() {
		this.brand.call();
	}
	
}
