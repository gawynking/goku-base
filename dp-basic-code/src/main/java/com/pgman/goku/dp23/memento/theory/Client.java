package com.pgman.goku.dp23.memento.theory;

public class Client {

    public static void main(String[] args) {

        Originator originator = new Originator();
        Caretaker caretaker = new Caretaker();

        originator.setState(" 100 ");

        caretaker.add(originator.saveStateMemento());

        originator.setState(" 80 ");

        caretaker.add(originator.saveStateMemento());

        originator.setState(" 50 ");
        caretaker.add(originator.saveStateMemento());


        System.out.println("current state = " + originator.getState());

        originator.getStateFromMemento(caretaker.get(0));
        System.out.println("==================================");
        System.out.println("history state =" + originator.getState());

    }

}
