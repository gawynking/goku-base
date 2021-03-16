package com.pgman.goku.dp23.factory.method.pizzastore.order;


import com.pgman.goku.dp23.factory.method.pizzastore.pizza.LDCheesePizza;
import com.pgman.goku.dp23.factory.method.pizzastore.pizza.LDPepperPizza;
import com.pgman.goku.dp23.factory.method.pizzastore.pizza.Pizza;

public class LDOrderPizza extends OrderPizza {

	
	@Override
	Pizza createPizza(String orderType) {
	
		Pizza pizza = null;
		if(orderType.equals("cheese")) {
			pizza = new LDCheesePizza();
		} else if (orderType.equals("pepper")) {
			pizza = new LDPepperPizza();
		}
		return pizza;
	}

}
