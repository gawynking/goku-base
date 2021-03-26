package com.pgman.goku.dp23.visitor;

/**
 * 将观众分为男人和女人，对歌手进行测评，当看完某个歌手表演后，得到他们对该歌手不同的评价(评价 有不同的种类，比如 成功、失败 等)
 */
public class Client {

	public static void main(String[] args) {

		ObjectStructure objectStructure = new ObjectStructure();
		
		objectStructure.attach(new Man());
		objectStructure.attach(new Woman());
		

		Success success = new Success();
		objectStructure.display(success);
		
		System.out.println("===============");
		Fail fail = new Fail();
		objectStructure.display(fail);
		
		System.out.println("===============");
		
		Wait wait = new Wait();
		objectStructure.display(wait);

	}

}
