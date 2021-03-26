package com.pgman.goku.dp23.visitor;

public class Wait extends Action {

	@Override
	public void getManResult(Man man) {
		System.out.println(" main wait ");
	}

	@Override
	public void getWomanResult(Woman woman) {
		System.out.println(" woman wait ");
	}

}
