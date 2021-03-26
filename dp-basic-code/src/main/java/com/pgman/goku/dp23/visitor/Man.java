package com.pgman.goku.dp23.visitor;

public class Man extends Person {

	@Override
	public void accept(Action action) {
		action.getManResult(this);
	}

}
