package com.pgman.goku.dp23.decorator;

/**
 * 星巴克咖啡订单项目（咖啡馆）：
 * 1) 咖啡种类/单品咖啡：Espresso(意大利浓咖啡)、ShortBlack、LongBlack(美式咖啡)、Decaf(无因咖啡)
 * 2) 调料：Milk、Soy(豆浆)、Chocolate
 * 3) 要求在扩展新的咖啡种类时，具有良好的扩展性、改动方便、维护方便
 * 4) 使用OO的来计算不同种类咖啡的费用: 客户可以点单品咖啡，也可以单品咖啡+调料组合。
 */
public class CoffeeBar {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // 1. LongBlack
        Drink order = new LongBlack();
        System.out.println("LongBlack cost=" + order.cost());
        System.out.println("LongBlack desc=" + order.getDes());

        // 2. order + 牛奶
        order = new Milk(order);

        System.out.println("order + Milk =" + order.cost());
        System.out.println("order + Milk = " + order.getDes());

        // 3. order + 巧克力
        order = new Chocolate(order);

        System.out.println("order + Milk + Chocolate =" + order.cost());
        System.out.println("order + Milk + Chocolate  = " + order.getDes());

        // 3. order + 巧克力
        order = new Chocolate(order);

        System.out.println("order + Milk + Chocolate*2  =" + order.cost());
        System.out.println("order + Milk + Chocolate*2  = " + order.getDes());

        System.out.println("===========================");
        Drink order2 = new DeCaf();

        System.out.println("order2 =" + order2.cost());
        System.out.println("order2 = " + order2.getDes());

        order2 = new Milk(order2);

        System.out.println("order2 + Milk =" + order2.cost());
        System.out.println("order2 + Milk = " + order2.getDes());

    }

}
