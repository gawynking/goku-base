package com.pgman.goku.dp23.factory.method.pizzastore.order;


import com.pgman.goku.dp23.factory.method.pizzastore.pizza.BJCheesePizza;
import com.pgman.goku.dp23.factory.method.pizzastore.pizza.BJPepperPizza;
import com.pgman.goku.dp23.factory.method.pizzastore.pizza.Pizza;

public class BJOrderPizza extends OrderPizza {

	
	@Override
	Pizza createPizza(String orderType) {
	
		Pizza pizza = null;
		if(orderType.equals("cheese")) {
			pizza = new BJCheesePizza();
		} else if (orderType.equals("pepper")) {
			pizza = new BJPepperPizza();
		}
		return pizza;
	}

}
