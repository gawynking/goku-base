package com.pgman.goku.dp23.mediator.smarthouse;

import java.util.HashMap;


public class ConcreteMediator extends Mediator {

    private HashMap<String, Colleague> colleagueMap;
    private HashMap<String, String> interMap;

    public ConcreteMediator() {
        colleagueMap = new HashMap<String, Colleague>();
        interMap = new HashMap<String, String>();
    }

    @Override
    public void Register(String colleagueName, Colleague colleague) {

        colleagueMap.put(colleagueName, colleague);

        if (colleague instanceof Alarm) { // 闹铃
            interMap.put("Alarm", colleagueName);
        } else if (colleague instanceof CoffeeMachine) { // 咖啡机
            interMap.put("CoffeeMachine", colleagueName);
        } else if (colleague instanceof TV) { // 电视机
            interMap.put("TV", colleagueName);
        } else if (colleague instanceof Curtains) { // 窗帘
            interMap.put("Curtains", colleagueName);
        }

    }

    @Override
    public void GetMessage(int stateChange, String colleagueName) {

        if (colleagueMap.get(colleagueName) instanceof Alarm) { // 闹铃

            if (stateChange == 0) {
                ((CoffeeMachine) (colleagueMap.get(interMap.get("CoffeeMachine")))).StartCoffee();
                ((TV) (colleagueMap.get(interMap.get("TV")))).StartTv();
            } else if (stateChange == 1) {
                ((TV) (colleagueMap.get(interMap.get("TV")))).StopTv();
            }

        } else if (colleagueMap.get(colleagueName) instanceof CoffeeMachine) { // 咖啡机

            ((Curtains) (colleagueMap.get(interMap.get("Curtains")))).UpCurtains();

        } else if (colleagueMap.get(colleagueName) instanceof TV) { // 电视机

        } else if (colleagueMap.get(colleagueName) instanceof Curtains) { // 窗帘

        }

    }

    @Override
    public void SendMessage() {

    }

}
