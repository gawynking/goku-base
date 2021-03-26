package com.pgman.goku.dp23.state.money;

/**
 * 借贷平台的订单，有审核-发布-抢单 等等 步骤，随着操作的不同，会改变订单的状态, 项目中的这个模块实现就会使用到状态模式
 */
public class ClientTest {

    public static void main(String[] args) {

        Context context = new Context();
        context.setState(new PublishState());
        System.out.println(context.getCurrentState());

        context.acceptOrderEvent(context);
        context.payOrderEvent(context);

    }

}
