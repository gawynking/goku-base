package com.pgman.goku.dp23.adapter.bridge;


//�۵�ʽ�ֻ��࣬�̳� ������ Phone
public class FoldedPhone extends Phone {

	//������
	public FoldedPhone(Brand brand) {
		super(brand);
	}
	
	public void open() {
		super.open();
		System.out.println(" �۵���ʽ�ֻ� ");
	}
	
	public void close() {
		super.close();
		System.out.println(" �۵���ʽ�ֻ� ");
	}
	
	public void call() {
		super.call();
		System.out.println(" �۵���ʽ�ֻ� ");
	}
}
