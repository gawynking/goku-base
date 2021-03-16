package com.pgman.goku.dp23.factory.none.order;

/**
 * 无设计模式方案
 *
 * 1) 优点是比较好理解，简单易操作。
 * 2) 缺点是违反了设计模式的ocp原则，即对扩展开放，对修改关闭。即当我们给类增加新功能的时候，尽量不修改代码，或者尽可能少修改代码.
 *
 * 改进的思路分析
 * 分析：修改代码可以接受，但是如果我们在其它的地方也有创建Pizza的代码，就意味
 * 着，也需要修改，而创建Pizza的代码，往往有多处。
 * 思路：把创建Pizza对象封装到一个类中，这样我们有新的Pizza种类时，只需要修改该
 * 类就可，其它有创建到Pizza对象的代码就不需要修改了.-> 简单工厂模式
 */
public class PizzaStore {

	public static void main(String[] args) {

		new OrderPizza();

	}

}
