package com.pgman.goku.dp23.observer.improve;

public class BaiduSite implements Observer {

    private float temperature;
    private float pressure;
    private float humidity;

    public void update(float temperature, float pressure, float humidity) {
        this.temperature = temperature;
        this.pressure = pressure;
        this.humidity = humidity;
        display();
    }

    public void display() {
        System.out.println("***baidu Today mTemperature: " + temperature + "***");
        System.out.println("***baidu Today mPressure: " + pressure + "***");
        System.out.println("***baidu Today mHumidity: " + humidity + "***");
    }

}
