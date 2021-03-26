package com.pgman.goku.dp23.state.state;

/**
 * 不能抽奖状态类
 */
public class NoRaffleState extends State {

    RaffleActivity activity;

    public NoRaffleState(RaffleActivity activity) {
        this.activity = activity;
    }

    @Override
    public void deductMoney() {
        System.out.println("-50");
        activity.setState(activity.getCanRaffleState());
    }

    @Override
    public boolean raffle() {
        System.out.println("NoRaffleState no 1");
        return false;
    }

    @Override
    public void dispensePrize() {
        System.out.println("NoRaffleState no 2");
    }

}
