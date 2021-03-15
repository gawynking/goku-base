package com.pgman.goku.dp23.singleton.type8;

/**
 * 使用枚举实现单例 ： 推荐使用
 */
public class SingletonType8 {
	public static void main(String[] args) {
		Singleton instance = Singleton.INSTANCE;
		Singleton instance2 = Singleton.INSTANCE;
		System.out.println(instance == instance2);
		System.out.println(instance.hashCode());
		System.out.println(instance2.hashCode());
		instance.sayOK();
	}
}

//使用枚举，可以实现单例, 推荐
enum Singleton {
	INSTANCE; // 属性
	public void sayOK() {
		System.out.println("ok~");
	}
}