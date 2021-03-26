package com.pgman.goku.dp23.proxy.dynamic;

public class TeacherDao implements ITeacherDao {

	@Override
	public void teach() {
		System.out.println(" teaching ");
	}

	@Override
	public void sayHello(String name) {
		System.out.println("hello " + name);
	}
	
}
