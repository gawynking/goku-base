package com.pgman.goku.dp23.flyweight;

public class Client {

	public static void main(String[] args) {

		WebSiteFactory factory = new WebSiteFactory();

		WebSite webSite1 = factory.getWebSiteCategory("red");

		
		webSite1.use(new User("tom"));

		WebSite webSite2 = factory.getWebSiteCategory("blue");

		webSite2.use(new User("jack"));

		WebSite webSite3 = factory.getWebSiteCategory("grew");

		webSite3.use(new User("smith"));

		WebSite webSite4 = factory.getWebSiteCategory("yellow");

		webSite4.use(new User("king"));
		
		System.out.println(" object = " + factory.getWebSiteCount());
	}

}
