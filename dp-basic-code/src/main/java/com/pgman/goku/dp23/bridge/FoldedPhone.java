package com.pgman.goku.dp23.bridge;


public class FoldedPhone extends Phone {

	public FoldedPhone(Brand brand) {
		super(brand);
	}
	
	public void open() {
		super.open();
		System.out.println(" FoldedPhone brand open ");
	}
	
	public void close() {
		super.close();
		System.out.println(" FoldedPhone brand close ");
	}
	
	public void call() {
		super.call();
		System.out.println(" FoldedPhone brand call ");
	}

}
