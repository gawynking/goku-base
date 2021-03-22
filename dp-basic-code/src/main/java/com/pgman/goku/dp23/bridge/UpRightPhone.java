package com.pgman.goku.dp23.bridge;

public class UpRightPhone extends Phone {

		public UpRightPhone(Brand brand) {
			super(brand);
		}
		
		public void open() {
			super.open();
			System.out.println(" UpRightPhone brand open ");
		}
		
		public void close() {
			super.close();
			System.out.println(" UpRightPhone brand open ");
		}
		
		public void call() {
			super.call();
			System.out.println(" UpRightPhone brand open ");
		}
}
