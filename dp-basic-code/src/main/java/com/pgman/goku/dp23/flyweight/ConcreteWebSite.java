package com.pgman.goku.dp23.flyweight;

public class ConcreteWebSite extends WebSite {

	private String type = "";

	public ConcreteWebSite(String type) {
		this.type = type;
	}

	@Override
	public void use(User user) {
		System.out.println("website type : " + type + " user is : " + user.getName());
	}

}
