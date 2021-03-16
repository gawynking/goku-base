package com.pgman.goku.dp23.factory.abs.pizzastore.order;


import com.pgman.goku.dp23.factory.abs.pizzastore.pizza.BJCheesePizza;
import com.pgman.goku.dp23.factory.abs.pizzastore.pizza.BJPepperPizza;
import com.pgman.goku.dp23.factory.abs.pizzastore.pizza.Pizza;

public class BJFactory implements AbsFactory {

	@Override
	public Pizza createPizza(String orderType) {
		System.out.println("~抽象工厂方法~");
		Pizza pizza = null;
		if(orderType.equals("cheese")) {
			pizza = new BJCheesePizza();
		} else if (orderType.equals("pepper")){
			pizza = new BJPepperPizza();
		}
		return pizza;
	}

}
