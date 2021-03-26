package com.pgman.goku.dp23.template.runoob;

public class TemplatePatternDemo {
    public static void main(String[] args) {

        Game game = new Cricket();
        game.play();
        System.out.println("------------------------");
        game = new Football();
        game.play();

    }
}
