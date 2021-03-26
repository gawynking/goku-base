package com.pgman.goku.dp23.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InfoCollege implements College {

	List<Department> departmentList;
	
	public InfoCollege() {
		departmentList = new ArrayList<Department>();
		addDepartment("info", " info ");
		addDepartment("web", " web ");
		addDepartment("database", " database ");
	}
	
	@Override
	public String getName() {
		return "Info";
	}

	@Override
	public void addDepartment(String name, String desc) {
		Department department = new Department(name, desc);
		departmentList.add(department);
	}

	@Override
	public Iterator createIterator() {
		return new InfoColleageIterator(departmentList);
	}

}
