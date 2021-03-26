package com.pgman.goku.dp23.mediator.smarthouse;

/**
 * 智能家庭项目：
 * 1) 智能家庭包括各种设备，闹钟、咖啡机、电视机、窗帘 等
 * 2) 主人要看电视时，各个设备可以协同工作，自动完成看电视的准备工作，比如流程为：闹铃响起->咖啡机开始做咖啡->窗帘自动落下->电视机开始播放
 */
public class Client {
    public static void main(String[] args) {

        Mediator mediator = new ConcreteMediator();

        Alarm alarm = new Alarm(mediator, "alarm");
        CoffeeMachine coffeeMachine = new CoffeeMachine(mediator, "coffeeMachine");
        Curtains curtains = new Curtains(mediator, "curtains");
        TV tV = new TV(mediator, "TV");

        alarm.SendAlarm(0);
        coffeeMachine.FinishCoffee();
        alarm.SendAlarm(1);

    }
}
