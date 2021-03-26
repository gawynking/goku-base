package com.pgman.goku.dp23.proxy.staticproxy;

public class TeacherDao implements ITeacherDao {

	@Override
	public void teach() {
		System.out.println(" teacher is running ");
	}

}
