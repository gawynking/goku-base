package com.pgman.goku.dp23.command;

public class Client {

    public static void main(String[] args) {

        // 初始化控制器
        RemoteController remoteController = new RemoteController();

        // 电灯
        LightReceiver lightReceiver = new LightReceiver();

        LightOnCommand lightOnCommand = new LightOnCommand(lightReceiver);
        LightOffCommand lightOffCommand = new LightOffCommand(lightReceiver);

        remoteController.setCommand(0, lightOnCommand, lightOffCommand);

        System.out.println("-------- light on -----------");
        remoteController.onButtonWasPushed(0);
        System.out.println("-------- light off -----------");
        remoteController.offButtonWasPushed(0);
        System.out.println("-------- light undo -----------");
        remoteController.undoButtonWasPushed();


        System.out.println("===================");

        TVReceiver tvReceiver = new TVReceiver();

        TVOffCommand tvOffCommand = new TVOffCommand(tvReceiver);
        TVOnCommand tvOnCommand = new TVOnCommand(tvReceiver);

        remoteController.setCommand(1, tvOnCommand, tvOffCommand);

        System.out.println("-------- tv on -----------");
        remoteController.onButtonWasPushed(1);
        System.out.println("-------- tv off -----------");
        remoteController.offButtonWasPushed(1);
        System.out.println("-------- tv undo -----------");
        remoteController.undoButtonWasPushed();

    }

}
