package com.pgman.goku.dp23.memento.game;

public class GameRole {

    private int vit;
    private int def;

    public Memento createMemento() {
        return new Memento(vit, def);
    }

    public void recoverGameRoleFromMemento(Memento memento) {
        this.vit = memento.getVit();
        this.def = memento.getDef();
    }

    public void display() {
        System.out.println(" vit is : " + this.vit + " def is : " + this.def);
    }

    public int getVit() {
        return vit;
    }

    public void setVit(int vit) {
        this.vit = vit;
    }

    public int getDef() {
        return def;
    }

    public void setDef(int def) {
        this.def = def;
    }

}
