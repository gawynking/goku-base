package com.pgman.goku.dp23.strategy.none;

public class ToyDuck extends Duck{

	@Override
	public void display() {
		System.out.println("ToyDuck");
	}

	public void quack() {
		System.out.println("ToyDuck quack");
	}
	
	public void swim() {
		System.out.println("ToyDuck swim");
	}
	
	public void fly() {
		System.out.println("ToyDuck fly");
	}

}
