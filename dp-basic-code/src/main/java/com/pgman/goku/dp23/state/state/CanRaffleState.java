package com.pgman.goku.dp23.state.state;

import java.util.Random;

/**
 * 可以抽奖类
 */
public class CanRaffleState extends State {

    RaffleActivity activity;

    public CanRaffleState(RaffleActivity activity) {
        this.activity = activity;
    }

    @Override
    public void deductMoney() {
        System.out.println("CanRaffleState no 1");
    }

    @Override
    public boolean raffle() {
        System.out.println("dispensePrize running ... ...");
        Random r = new Random();
        int num = r.nextInt(10);

        if (num == 0) {
            activity.setState(activity.getDispenseState());
            return true;
        } else {
            activity.setState(activity.getNoRafflleState());
            return false;
        }
    }

    @Override
    public void dispensePrize() {
        System.out.println("CanRaffleState no 2");
    }

}
