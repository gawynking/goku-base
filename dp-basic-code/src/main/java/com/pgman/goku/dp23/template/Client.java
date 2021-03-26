package com.pgman.goku.dp23.template;

public class Client {

	public static void main(String[] args) {
		
		System.out.println("----RedBeanSoyaMilk----");
		SoyaMilk redBeanSoyaMilk = new RedBeanSoyaMilk();
		redBeanSoyaMilk.make();
		
		System.out.println("----PeanutSoyaMilk----");
		SoyaMilk peanutSoyaMilk = new PeanutSoyaMilk();
		peanutSoyaMilk.make();

		System.out.println("----PureSoyaMilk----");
		SoyaMilk pureSoyaMilk = new PureSoyaMilk();
		pureSoyaMilk.make();

	}

}
