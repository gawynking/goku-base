package com.pgman.goku.dp23.prototype.none;

/**
 * 问题：现在有一只羊tom，姓名为: tom, 年龄为：1，颜色为：白色，请编写程序创建和tom羊 属性完全相同的10只羊。
 *
 *
 * 传统的方式的优缺点
 * 1) 优点是比较好理解，简单易操作。
 * 2) 在创建新的对象时，总是需要重新获取原始对象的属性，如果创建的对象比较复杂
 * 时，效率较低
 * 3) 总是需要重新初始化对象，而不是动态地获得对象运行时的状态, 不够灵活
 *
 * 思路：Java中Object类是所有类的根类，Object类提供了一个clone()方法，该方法可以
 * 将一个Java对象复制一份，但是需要实现clone的Java类必须要实现一个接口Cloneable，
 * 该接口表示该类能够复制且具有复制的能力 => 原型模式
 */
public class Client {

	public static void main(String[] args) {

		Sheep sheep = new Sheep("tom", 1, "白色");
		Sheep sheep2 = new Sheep(sheep.getName(), sheep.getAge(), sheep.getColor());
		Sheep sheep3 = new Sheep(sheep.getName(), sheep.getAge(), sheep.getColor());
		Sheep sheep4 = new Sheep(sheep.getName(), sheep.getAge(), sheep.getColor());
		Sheep sheep5 = new Sheep(sheep.getName(), sheep.getAge(), sheep.getColor());

		System.out.println(sheep);
		System.out.println(sheep2);
		System.out.println(sheep3);
		System.out.println(sheep4);
		System.out.println(sheep5);

	}

}
