package com.pgman.goku.dp23.strategy.improve;

public class WildDuck extends Duck {

	public  WildDuck() {
		flyBehavior = new GoodFlyBehavior();
	}

	@Override
	public void display() {
		System.out.println(" WildDuck ");
	}

}
