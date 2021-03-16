package com.pgman.goku.dp23.factory.abs.pizzastore.order;


import com.pgman.goku.dp23.factory.abs.pizzastore.pizza.Pizza;

public interface AbsFactory {
	public Pizza createPizza(String orderType);
}
