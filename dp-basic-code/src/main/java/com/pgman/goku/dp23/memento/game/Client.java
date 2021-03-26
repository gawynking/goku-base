package com.pgman.goku.dp23.memento.game;

/**
 * 游戏角色状态恢复问题
 * 游戏角色有攻击力和防御力，在大战Boss前保存自身的状态(攻击力和防御力)，当大
 * 战Boss后攻击力和防御力下降，从备忘录对象恢复到大战前的状态
 */
public class Client {

    public static void main(String[] args) {

        GameRole gameRole = new GameRole();
        gameRole.setVit(100);
        gameRole.setDef(100);

        System.out.println("-----1-----");
        gameRole.display();

        Caretaker caretaker = new Caretaker();
        caretaker.setMemento(gameRole.createMemento());

        System.out.println("-----2-----");
        gameRole.setDef(30);
        gameRole.setVit(30);

        gameRole.display();

        gameRole.recoverGameRoleFromMemento(caretaker.getMemento());
        System.out.println("-----3-----");
        gameRole.display();

    }

}
