package com.pgman.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.mapper.*;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.KafkaUtils;
import com.pgman.goku.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class MockOrderData {

    public static boolean cycleFlag = true;

    public static void mockOrder(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        // 基础数据
        List<UserMapper> users = UserMapper.users;
        List<BookShopMapper> bookShops = BookShopMapper.bookShops;
        List<BookMapper> books = BookMapper.books;


        int orderId = 0;
        int shopId;
        int userId;
        String createDateTime;

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
            shopId = bookShops.get(new Random().nextInt(bookShops.size())).getShopId();
            userId = users.get(new Random().nextInt(users.size())).getUserId();
            createDateTime = DateUtils.getCurrentDatetime();

            List<BookMapper> innerBooks = new ArrayList<>();
            innerBooks.addAll(books);
            innerBooks.sort(new Comparator<BookMapper>() {
                @Override
                public int compare(BookMapper o1, BookMapper o2) {
                    return new Random().nextInt(9) - new Random().nextInt(9);
                }
            });
            int bookTypeNumber = new Random().nextInt(innerBooks.size() > 3 ? 3 : innerBooks.size());
            for (int j = 0; j <= bookTypeNumber; j++) {
                BookMapper tmpBook = innerBooks.get(j);

                orderBookId = orderBookId + 1;
                bookId = tmpBook.getBookId();
                bookNumber = new Random().nextInt(3) + 1;

                bookOriginalPrice = new BigDecimal(tmpBook.getPrice()).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                bookDiscountPrice = new BigDecimal(bookOriginalPrice * (new Random().nextInt(5) / 10.0f)).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
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
//                        System.out.println(jsonOrder.toString());
                        KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"),jsonOrder.toString());
                        for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                            JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
//                            System.out.println(jsonOrderDetail.toString());
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"),jsonOrderDetail.toString());
                        }

                        Thread.sleep(new Random().nextInt(sleepTime));
                    }
                } else {
                    cache.sort(new Comparator<Order>() {
                        @Override
                        public int compare(Order o1, Order o2) {
                            return new Random().nextInt() - new Random().nextInt();
                        }
                    });
                    for (Order entry : cache) {
                        OrderMapper orderMapper1 = entry.getOrderMapper();
                        List<OrderDetailMapper> orderDetailMappers1 = entry.getOrderDetailMappers();

                        if(new Random().nextInt(99)%2==0) {
                            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"), jsonOrder.toString());

                            Thread.sleep(new Random().nextInt(sleepTime));

                            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
                                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"), jsonOrderDetail.toString());
                                Thread.sleep(new Random().nextInt(sleepTime));
                            }
                        }else {
                            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
                                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"), jsonOrderDetail.toString());
                                Thread.sleep(new Random().nextInt(sleepTime));
                            }

                            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"), jsonOrder.toString());
                        }

                        Thread.sleep(new Random().nextInt(sleepTime));
                    }
                }
                cache.clear();
                cache.add(order);
                i++;
            }

        }

        for (Order entry : cache) {
            OrderMapper orderMapper1 = entry.getOrderMapper();
            List<OrderDetailMapper> orderDetailMappers1 = entry.getOrderDetailMappers();

            JSONObject jsonOrder = ObjectUtils.objInstanceToJsonObject(orderMapper1, OrderMapper.class);
//            System.out.println(jsonOrder.toString());
            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"),jsonOrder.toString());
            for (OrderDetailMapper orderDetailMapper : orderDetailMappers1) {
                JSONObject jsonOrderDetail = ObjectUtils.objInstanceToJsonObject(orderDetailMapper, OrderDetailMapper.class);
//                System.out.println(jsonOrderDetail.toString());
                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.book.topics"),jsonOrderDetail.toString());
            }

            Thread.sleep(new Random().nextInt(sleepTime));
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
