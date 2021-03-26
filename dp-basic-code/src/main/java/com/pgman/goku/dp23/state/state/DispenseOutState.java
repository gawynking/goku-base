package com.pgman.goku.dp23.state.state;

/**
 * 奖品发放完毕
 */
public class DispenseOutState extends State {

    RaffleActivity activity;

    public DispenseOutState(RaffleActivity activity) {
        this.activity = activity;
    }

    @Override
    public void deductMoney() {
        System.out.println("DispenseOutState 1");
    }

    @Override
    public boolean raffle() {
        System.out.println("DispenseOutState 2");
        return false;
    }

    @Override
    public void dispensePrize() {
        System.out.println("DispenseOutState 3");
    }

}
