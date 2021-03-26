package com.pgman.goku.dp23.state.state;

/**
 * 请编写程序完成APP抽奖活动 具体要求如下:
 * 1) 假如每参加一次这个活动要扣除用户50积分，中奖概率是10%
 * 2) 奖品数量固定，抽完就不能抽奖
 * 3) 活动有四个状态: 可以抽奖、不能抽奖、发放奖品和奖品领完
 */
public class ClientTest {

    public static void main(String[] args) {

        RaffleActivity activity = new RaffleActivity(1);

        for (int i = 0; i < 30; i++) {
            System.out.println("-------- Num " + (i + 1) + " ----------");
            activity.debuctMoney();
            activity.raffle();
        }
    }

}
