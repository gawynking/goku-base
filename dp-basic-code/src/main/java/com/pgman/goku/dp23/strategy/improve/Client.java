package com.pgman.goku.dp23.strategy.improve;

public class Client {

	public static void main(String[] args) {

		WildDuck wildDuck = new WildDuck();
		wildDuck.fly();
		
		ToyDuck toyDuck = new ToyDuck();
		toyDuck.fly();
		
		PekingDuck pekingDuck = new PekingDuck();
		pekingDuck.fly();
		
		pekingDuck.setFlyBehavior(new NoFlyBehavior());
		System.out.println("--------------------------");
		pekingDuck.fly();

	}

}
