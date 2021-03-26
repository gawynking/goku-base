package com.pgman.goku.dp23.visitor;

public class Success extends Action {

	@Override
	public void getManResult(Man man) {
		System.out.println(" man success ");
	}

	@Override
	public void getWomanResult(Woman woman) {
		System.out.println(" woman success ");
	}

}
