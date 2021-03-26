package com.pgman.goku.dp23.state.state;

/**
 * 状态接口
 */
public abstract class State {

    public abstract void deductMoney();
    public abstract boolean raffle();
    public abstract  void dispensePrize();

}
