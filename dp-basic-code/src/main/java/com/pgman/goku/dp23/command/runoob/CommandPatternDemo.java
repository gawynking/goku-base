package com.pgman.goku.dp23.command.runoob;

public class CommandPatternDemo {
    public static void main(String[] args) {

        Stock abcStock = new Stock();

        BuyStock buyStockOrder = new BuyStock(abcStock);
        SellStock sellStockOrder = new SellStock(abcStock);

        Broker broker = new Broker();

        // 新建命令
        broker.takeOrder(buyStockOrder);
        broker.takeOrder(sellStockOrder);

        // 执行命令
        broker.placeOrders();

    }
}
