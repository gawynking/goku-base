package com.pgman.goku.dp23.visitor;

public class Fail extends Action {

	@Override
	public void getManResult(Man man) {
		System.out.println(" man fail ");
	}

	@Override
	public void getWomanResult(Woman woman) {
		System.out.println(" woman fail ");
	}

}
