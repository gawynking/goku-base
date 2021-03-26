package com.pgman.goku.dp23.memento.theory;

import java.util.ArrayList;
import java.util.List;

public class Caretaker {
	
	private List<Memento> mementoList = new ArrayList<Memento>();
	
	public void add(Memento memento) {
		mementoList.add(memento);
	}
	public Memento get(int index) {
		return mementoList.get(index);
	}

}
