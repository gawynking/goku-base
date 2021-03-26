package com.pgman.goku.dp23.iterator;

import java.util.Iterator;

public interface College {
	
	public String getName();
	public void addDepartment(String name, String desc);
	public Iterator  createIterator();

}
