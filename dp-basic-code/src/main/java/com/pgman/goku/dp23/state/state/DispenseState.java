package com.pgman.goku.dp23.state.state;

/**
 * 发放奖品状态
 */
public class DispenseState extends State {

    RaffleActivity activity;

    public DispenseState(RaffleActivity activity) {
        this.activity = activity;
    }

    @Override
    public void deductMoney() {
        System.out.println("DispenseState no 1");
    }

    @Override
    public boolean raffle() {
        System.out.println("DispenseState no 1");
        return false;
    }

    @Override
    public void dispensePrize() {
        if (activity.getCount() > 0) {
            System.out.println("DispenseState fail");
            activity.setState(activity.getNoRafflleState());
        } else {
            System.out.println("DispenseState success");
            activity.setState(activity.getDispensOutState());
        }
    }

}
