package com.pgman.goku.dp23.observer.none;

/**
 * 天气预报项目需求,具体要求如下：
 * 1) 气象站可以将每天测量到的温度，湿度，气压等等以公告的形式发布出去(比如发布到自己的网站或第三方)。
 * 2) 需要设计开放型API，便于其他第三方也能接入气象站获取数据。
 * 3) 提供温度、气压和湿度的接口
 * 4) 测量数据更新时，要能实时的通知给第三方
 */
public class Client {
	public static void main(String[] args) {

		CurrentConditions currentConditions = new CurrentConditions();
		WeatherData weatherData = new WeatherData(currentConditions);
		
		weatherData.setData(30, 150, 40);
		weatherData.setData(40, 160, 20);

	}
}
