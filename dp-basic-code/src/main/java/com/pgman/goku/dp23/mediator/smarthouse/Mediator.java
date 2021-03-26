package com.pgman.goku.dp23.mediator.smarthouse;


// 中介者类
public abstract class Mediator {

    // 将同事对象注册进来
    public abstract void Register(String colleagueName, Colleague colleague);

    public abstract void GetMessage(int stateChange, String colleagueName);

    public abstract void SendMessage();

}
