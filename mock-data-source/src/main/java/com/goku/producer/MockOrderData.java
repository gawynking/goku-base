package com.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.goku.config.ConfigurationManager;
import com.goku.mapper.*;
import com.goku.util.DateUtils;
import com.goku.util.KafkaUtils;
import com.goku.util.MysqlJDBCUtils;
import com.goku.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.*;

public class MockOrderData {

    private static String orderCountSQL = "select count(1) as cnt from tbl_order where order_id = ?";
    private static String orderInsertSQL = "insert into tbl_order(\n" +
            "    order_id,\n" +
            "    shop_id,\n" +
            "    user_id,\n" +
            "    original_price,\n" +
            "    actual_price,\n" +
            "    discount_price,\n" +
            "    order_status,\n" +
            "    create_time \n" +
            ")value(\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?\n" +
            ")";
    private static String orderDetailCountSQL = "select count(1) as cnt from tbl_order_detail where order_book_id = ?";
    private static String orderDetailInsertSQL = "insert into tbl_order_detail(\n" +
            "    order_book_id,\n" +
            "    order_id,\n" +
            "    book_id,\n" +
            "    book_number,\n" +
            "    original_price,\n" +
            "    actual_price,\n" +
            "    discount_price,\n" +
            "    create_time \n" +
            ")value(\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?,\n" +
            "    ?\n" +
            ")";

    public static boolean cycleFlag = true;

    public static void mockOrder(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        Random random = new Random();

        // 基础数据
        List<UserMapper> users = UserMapper.users;
        List<BookShopMapper> bookShops = BookShopMapper.bookShops;
        List<BookMapper> books = BookMapper.books;


        int orderId = 0;
        int shopId;
        int userId;
        String createDateTime;
        int orderStatus = 1;

        int orderBookId = 0;
        int bookId;
        int bookNumber = 0;
        double bookOriginalPrice = 0;
        double bookActualPrice = 0;
        double bookDiscountPrice = 0;

        double originalPrice = 0;
        double actualPrice = 0;
        double discountPrice = 0;


        List<Order> cache = new ArrayList<>(unOrderedNum);
        int i = 0;
        while (cycleFlag) {

            Order order = new Order();

            orderId = orderId + 1;
            shopId = bookShops.get(random.nextInt(bookShops.size())).getShopId();
            userId = users.get(random.nextInt(users.size())).getUserId();
            createDateTime = DateUtils.getCurrentDatetime();

            List<BookMapper> innerBooks = new ArrayList<>();
            innerBooks.addAll(books);
            Collections.shuffle(innerBooks);
            int bookTypeNumber = random.nextInt(innerBooks.size() > 3 ? 3 : innerBooks.size());
            for (int j = 0; j <= bookTypeNumber; j++) {
                BookMapper tmpBook = innerBooks.get(j);

                orderBookId = orderBookId + 1;
                bookId = tmpBook.getBookId();
                bookNumber = random.nextInt(3) + 1;

                bookOriginalPrice = new BigDecimal(tmpBook.getPrice()).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                bookDiscountPrice = new BigDecimal(bookOriginalPrice * (random.nextInt(5) / 10.0f)).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                bookActualPrice = new BigDecimal(bookOriginalPrice - bookDiscountPrice).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();

                order.orderDetailMappers.add(new OrderDetailMapper(
                        orderBookId,
                        orderId,
                        bookId,
                        bookOriginalPrice,
                        bookActualPrice,
                        bookDiscountPrice,
                        bookNumber,
                        createDateTime
                ));
            }

            List<OrderDetailMapper> orderDetailMappers = order.getOrderDetailMappers();
            for (OrderDetailMapper orderDetailMapper : orderDetailMappers) {
                originalPrice = new BigDecimal(originalPrice + orderDetailMapper.getOriginalPrice()*orderDetailMapper.getBookNumber()).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                actualPrice = new BigDecimal(actualPrice + orderDetailMapper.getActualPrice()*orderDetailMapper.getBookNumber()).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                discountPrice = new BigDecimal(discountPrice + orderDetailMapper.getDiscountPrice()*orderDetailMapper.getBookNumber()).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
            }

            order.setOrderMapper(new OrderMapper(
                    orderId,
                    shopId,
                    userId,
                    originalPrice,
                    actualPrice,
                    discountPrice,
                    orderStatus,
                    createDateTime
            ));

            originalPrice = 0;
            actualPrice = 0;
            discountPrice = 0;

            if (i < unOrderedNum) {
                cache.add(order);
                i++;
            } else {
                i = 0;

                if (orderedFlag) {
                    for (Order entry : cache) {
                        OrderMapper orderMapper1 = entry.getOrderMapper();
                        List<OrderDetailMapper> orderDetailMappers1 = entry.getOrderDetailMappers();

                        JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
                        if(!(MysqlJDBCUtils.getInstance().getCount(orderCountSQL,new Object[]{orderMapper1.getOrderId()}) > 0)) {
                            MysqlJDBCUtils.getInstance().executeUpdate(
                                    orderInsertSQL,
                                    new Object[]{
                                            orderMapper1.getOrderId(),
                                            orderMapper1.getShopId(),
                                            orderMapper1.getUserId(),
                                            orderMapper1.getOriginalPrice(),
                                            orderMapper1.getActualPrice(),
                                            orderMapper1.getDiscountPrice(),
                                            orderMapper1.getOrderStatus(),
                                            orderMapper1.getCreateTime()
                                    }
                            );
                        }
                        KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"),jsonOrder.toString());
                        for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                            JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);

                            if(!(MysqlJDBCUtils.getInstance().getCount(orderDetailCountSQL,new Object[]{orderDetailMapper.getOrderBookId()}) > 0)) {
                                MysqlJDBCUtils.getInstance().executeUpdate(
                                        orderDetailInsertSQL,
                                        new Object[]{
                                                orderDetailMapper.getOrderBookId(),
                                                orderDetailMapper.getOrderId(),
                                                orderDetailMapper.getBookId(),
                                                orderDetailMapper.getBookNumber(),
                                                orderDetailMapper.getOriginalPrice(),
                                                orderDetailMapper.getActualPrice(),
                                                orderDetailMapper.getDiscountPrice(),
                                                orderDetailMapper.getCreateTime()
                                        }
                                );
                            }

                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"),jsonOrderDetail.toString());
                        }
                    }
                } else {
                    Collections.shuffle(cache);
                    for (Order entry : cache) {
                        OrderMapper orderMapper1 = entry.getOrderMapper();
                        List<OrderDetailMapper> orderDetailMappers1 = entry.getOrderDetailMappers();

                        if(random.nextInt(99)%2==0) {
                            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
                            if(!(MysqlJDBCUtils.getInstance().getCount(orderCountSQL,new Object[]{orderMapper1.getOrderId()}) > 0)) {
                                MysqlJDBCUtils.getInstance().executeUpdate(
                                        orderInsertSQL,
                                        new Object[]{
                                                orderMapper1.getOrderId(),
                                                orderMapper1.getShopId(),
                                                orderMapper1.getUserId(),
                                                orderMapper1.getOriginalPrice(),
                                                orderMapper1.getActualPrice(),
                                                orderMapper1.getDiscountPrice(),
                                                orderMapper1.getOrderStatus(),
                                                orderMapper1.getCreateTime()
                                        }
                                );
                            }
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"), jsonOrder.toString());

                            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
                                if(!(MysqlJDBCUtils.getInstance().getCount(orderDetailCountSQL,new Object[]{orderDetailMapper.getOrderBookId()}) > 0)) {
                                    MysqlJDBCUtils.getInstance().executeUpdate(
                                            orderDetailInsertSQL,
                                            new Object[]{
                                                    orderDetailMapper.getOrderBookId(),
                                                    orderDetailMapper.getOrderId(),
                                                    orderDetailMapper.getBookId(),
                                                    orderDetailMapper.getBookNumber(),
                                                    orderDetailMapper.getOriginalPrice(),
                                                    orderDetailMapper.getActualPrice(),
                                                    orderDetailMapper.getDiscountPrice(),
                                                    orderDetailMapper.getCreateTime()
                                            }
                                    );
                                }
                                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"), jsonOrderDetail.toString());
                            }
                        }else {
                            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
                                if(!(MysqlJDBCUtils.getInstance().getCount(orderDetailCountSQL,new Object[]{orderDetailMapper.getOrderBookId()}) > 0)) {
                                    MysqlJDBCUtils.getInstance().executeUpdate(
                                            orderDetailInsertSQL,
                                            new Object[]{
                                                    orderDetailMapper.getOrderBookId(),
                                                    orderDetailMapper.getOrderId(),
                                                    orderDetailMapper.getBookId(),
                                                    orderDetailMapper.getBookNumber(),
                                                    orderDetailMapper.getOriginalPrice(),
                                                    orderDetailMapper.getActualPrice(),
                                                    orderDetailMapper.getDiscountPrice(),
                                                    orderDetailMapper.getCreateTime()
                                            }
                                    );
                                }
                                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"), jsonOrderDetail.toString());
                            }

                            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
                            if(!(MysqlJDBCUtils.getInstance().getCount(orderCountSQL,new Object[]{orderMapper1.getOrderId()}) > 0)) {
                                MysqlJDBCUtils.getInstance().executeUpdate(
                                        orderInsertSQL,
                                        new Object[]{
                                                orderMapper1.getOrderId(),
                                                orderMapper1.getShopId(),
                                                orderMapper1.getUserId(),
                                                orderMapper1.getOriginalPrice(),
                                                orderMapper1.getActualPrice(),
                                                orderMapper1.getDiscountPrice(),
                                                orderMapper1.getOrderStatus(),
                                                orderMapper1.getCreateTime()
                                        }
                                );
                            }
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"), jsonOrder.toString());
                        }

                    }
                }
                cache.clear();
                cache.add(order);
                i++;
            }
            Thread.sleep(random.nextInt(sleepTime));
        }

        for (Order entry : cache) {
            OrderMapper orderMapper1 = entry.getOrderMapper();
            List<OrderDetailMapper> orderDetailMappers1 = entry.getOrderDetailMappers();

            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
            if(!(MysqlJDBCUtils.getInstance().getCount(orderCountSQL,new Object[]{orderMapper1.getOrderId()}) > 0)) {
                MysqlJDBCUtils.getInstance().executeUpdate(
                        orderInsertSQL,
                        new Object[]{
                                orderMapper1.getOrderId(),
                                orderMapper1.getShopId(),
                                orderMapper1.getUserId(),
                                orderMapper1.getOriginalPrice(),
                                orderMapper1.getActualPrice(),
                                orderMapper1.getDiscountPrice(),
                                orderMapper1.getOrderStatus(),
                                orderMapper1.getCreateTime()
                        }
                );
            }
            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"),jsonOrder.toString());
            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
                if(!(MysqlJDBCUtils.getInstance().getCount(orderDetailCountSQL,new Object[]{orderDetailMapper.getOrderBookId()}) > 0)) {
                    MysqlJDBCUtils.getInstance().executeUpdate(
                            orderDetailInsertSQL,
                            new Object[]{
                                    orderDetailMapper.getOrderBookId(),
                                    orderDetailMapper.getOrderId(),
                                    orderDetailMapper.getBookId(),
                                    orderDetailMapper.getBookNumber(),
                                    orderDetailMapper.getOriginalPrice(),
                                    orderDetailMapper.getActualPrice(),
                                    orderDetailMapper.getDiscountPrice(),
                                    orderDetailMapper.getCreateTime()
                            }
                    );
                }
                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"),jsonOrderDetail.toString());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        mockOrder(true, 3000, 3);
    }


    public static class Order {

        private OrderMapper orderMapper;
        private List<OrderDetailMapper> orderDetailMappers;

        public Order() {
            this.orderDetailMappers = new ArrayList<>();
        }

        public Order(OrderMapper orderMapper, List<OrderDetailMapper> orderDetailMappers) {
            this.orderMapper = orderMapper;
            this.orderDetailMappers = orderDetailMappers;
        }

        public OrderMapper getOrderMapper() {
            return orderMapper;
        }

        public void setOrderMapper(OrderMapper orderMapper) {
            this.orderMapper = orderMapper;
        }

        public List<OrderDetailMapper> getOrderDetailMappers() {
            return orderDetailMappers;
        }

        public void setOrderDetailMappers(List<OrderDetailMapper> orderDetailMappers) {
            this.orderDetailMappers = orderDetailMappers;
        }
    }

}
