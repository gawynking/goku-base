package com.pgman.goku.dp23.memento.game;

import java.util.ArrayList;
import java.util.HashMap;

public class Caretaker {

    private Memento memento;
    private ArrayList<Memento> mementos;
    private HashMap<String, ArrayList<Memento>> rolesMementos;

    public Memento getMemento() {
        return memento;
    }

    public void setMemento(Memento memento) {
        this.memento = memento;
    }

}
