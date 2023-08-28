package com.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.goku.config.ConfigurationManager;
import com.goku.mapper.OrderMapper;
import com.goku.mapper.OrderPaymentMapper;
import com.goku.util.DateUtils;
import com.goku.util.KafkaUtils;
import com.goku.util.MysqlJDBCUtils;
import com.goku.util.ObjectUtils;

import java.sql.ResultSet;
import java.util.*;

public class MockOrderPaymentData {

    private static String countSQL = "select count(1) as cnt from tbl_order_payment where pay_id = ?";

    private static String insertSQL = "insert into tbl_order_payment(\n" +
            "    pay_id,\n" +
            "    order_id,\n" +
            "    payment_amount,\n" +
            "    payment_status,\n" +
            "    create_time\n" +
            ")value(\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?\n" +
            ")";

    private static String nonPaymentOrderSQL =
                    "select \n" +
                            "    t1.order_id,\n" +
                            "    t1.shop_id,\n" +
                            "    t1.user_id,\n" +
                            "    t1.original_price,\n" +
                            "    t1.actual_price,\n" +
                            "    t1.discount_price,\n" +
                            "    t1.order_status,\n" +
                            "    t1.create_time \n" +
                            "from tbl_order t1 \n" +
                            "left join tbl_order_payment t2 on t1.order_id = t2.order_id \n" +
                            "where t1.order_status = 1\n" +
                            "  and from_unixtime(unix_timestamp(t1.create_time)+10*60) >= current_timestamp()\n" +
                            "  and t2.pay_id is null \n" +
                            "order by t1.order_id asc";

    private static String updateOrderStatusSQL = "update tbl_order set order_status = 2 where order_id = ?";


    public static boolean cycleFlag = true;
    private static boolean testFlag = false;

    public static boolean isCycleFlag() {
        return cycleFlag;
    }

    public static void setCycleFlag(boolean cycleFlag) {
        MockOrderPaymentData.cycleFlag = cycleFlag;
    }


    /**
     * 生成支付数据
     *
     * @param orderedFlag
     * @param sleepTime
     * @param unOrderedNum
     * @throws InterruptedException
     */
    public static void mockOrderPayment(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        Random random = new Random();

//        定义必要变量
        int payId = 0;
        int orderId;
        double paymentAmount;
        int paymentStatus = 0;
        String createDataTime;

        List<OrderPaymentMapper> cache = new ArrayList<>(unOrderedNum);
        int i = 0;
        while (cycleFlag) {

//            缓存未支付订单集合
            List<OrderMapper> orders = new ArrayList<>();
//            获取10分钟内未支付的订单数据
            MysqlJDBCUtils.getInstance().executeQuery(nonPaymentOrderSQL, null, new MysqlJDBCUtils.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    while (rs.next()){
                        OrderMapper order = new OrderMapper(
                                rs.getInt(1),
                                rs.getInt(2),
                                rs.getInt(3),
                                rs.getDouble(4),
                                rs.getDouble(5),
                                rs.getDouble(6),
                                rs.getInt(7),
                                rs.getString(8)
                        );
                        orders.add(order);
                    }
                }
            });

            for(OrderMapper order:orders){

                int random100 = random.nextInt(100);

                payId = payId + 1;
                orderId = order.getOrderId();
                paymentAmount = order.getActualPrice();

                if(random100 <= 90) {
                    paymentStatus = 1;
                }else if(random100 > 90 && random100 < 96){
                    paymentStatus = 2;
                }else {
                    continue;
                }

                createDataTime = DateUtils.getCurrentDatetime();

                OrderPaymentMapper orderPayment = new OrderPaymentMapper(
                        payId,
                        orderId,
                        paymentAmount,
                        paymentStatus,
                        createDataTime
                );

                if(i<unOrderedNum){
                    cache.add(orderPayment);
                    i++;
                }else{
                    i=0;
                    if(!orderedFlag){
                        Collections.shuffle(cache);
                    }

                    for(OrderPaymentMapper entry :cache){
                        if(testFlag){
                            System.out.println(entry.toString());
                        }else {
                            JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, OrderPaymentMapper.class);

//                        将数据写入外部存储
                            if(!(MysqlJDBCUtils.getInstance().getCount(countSQL,new Object[]{entry.getPayId()}) > 0)) {
                                MysqlJDBCUtils.getInstance().executeUpdate(
                                        insertSQL,
                                        new Object[]{
                                                entry.getPayId(),
                                                entry.getOrderId(),
                                                entry.getPaymentAmount(),
                                                entry.getPaymentStatus(),
                                                entry.getCreateTime()
                                        }
                                );
                            }
                            if(entry.getPaymentStatus() == 1){
                                MysqlJDBCUtils.getInstance().executeUpdate(updateOrderStatusSQL,new Object[]{entry.getOrderId()});
                            }
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("payment.topics"), jsonObject.toString());
                        }
                    }

                    cache.clear();
                    cache.add(orderPayment);
                    i++;
                }
                Thread.sleep(random.nextInt(sleepTime/2));
            }

            for(OrderPaymentMapper entry :cache){
                if(testFlag){
                    System.out.println(entry.toString());
                }else {
                    JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, OrderPaymentMapper.class);

//                        将数据写入外部存储
                    if(!(MysqlJDBCUtils.getInstance().getCount(countSQL,new Object[]{entry.getPayId()}) > 0)) {
                        MysqlJDBCUtils.getInstance().executeUpdate(
                                insertSQL,
                                new Object[]{
                                        entry.getPayId(),
                                        entry.getOrderId(),
                                        entry.getPaymentAmount(),
                                        entry.getPaymentStatus(),
                                        entry.getCreateTime()
                                }
                        );
                    }
                    if(entry.getPaymentStatus() == 1){
                        MysqlJDBCUtils.getInstance().executeUpdate(updateOrderStatusSQL,new Object[]{entry.getOrderId()});
                    }
                    KafkaUtils.getInstance().send(ConfigurationManager.getString("payment.topics"), jsonObject.toString());
                }
            }

            cache.clear();
            Thread.sleep(random.nextInt(1000*2*60-10) + 1000*60);
        }

    }

    public static void main(String[] args) throws Exception{
        testFlag = false;
        new MockOrderPaymentData().mockOrderPayment(false,1000,3);
    }

}
