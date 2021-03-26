package com.pgman.goku.dp23.iterator;

import java.util.Iterator;

public class ComputerCollege implements College {

	Department[] departments;
	int numOfDepartment = 0 ;

	public ComputerCollege() {
		departments = new Department[5];
		addDepartment("Java", " Java ");
		addDepartment("PHP", " PHP ");
		addDepartment("C++", " C++ ");
		addDepartment("C", " C ");
		addDepartment("Python", " Python ");
	}

	@Override
	public String getName() {
		return "Computer";
	}

	@Override
	public void addDepartment(String name, String desc) {
		Department department = new Department(name, desc);
		departments[numOfDepartment] = department;
		numOfDepartment += 1;
	}

	@Override
	public Iterator createIterator() {
		return new ComputerCollegeIterator(departments);
	}

}
