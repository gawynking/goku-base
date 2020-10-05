package com.pgman.goku.util;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GenerateTestDataUtils {


    public static List<JSONObject> customers = null;
    public static List<JSONObject> products = null;


    /**
     * 生产 100 个用户数据
     * @return
     */
    public static List<JSONObject> genCustomerData(){

        Integer[] customerId = {1990,1995,2000};
        String[] addresses = {"北京","上海"};
        List<JSONObject> customers = new ArrayList<>();

        // 生产customer数据
        for(int i = 10;i <= 15; i++){

            JSONObject customer = new JSONObject();

            Integer id = i;
            String name = "User-" + i;
            String sex = new Random().nextInt(2) == 1?"男":"女";
            String birthday = customerId[new Random().nextInt(3)] + "-03-15";
            String address = addresses[new Random().nextInt(2)];

            Long createTimestamp = new Random().nextInt(2678399 + 1) + 1577808000L; // 2020-01-01 00:00:00 ~ 2020-01-31 23:59:59
            String createTime = DateUtils.fromUnixtime(Long.valueOf(createTimestamp), DateUtils.DATE_TIME_PATTERN);

            customer.put("id",id);
            customer.put("name",name);
            customer.put("sex",sex);
            customer.put("birthday",birthday);
            customer.put("address",address);
            customer.put("create_time",createTime);

            customers.add(customer);

        }

        return customers;

    }


    /**
     * 生产 10 个产品数据
     *
     * @return
     */
    public static List<JSONObject> genProductData(){

        String[] names = {"PostgreSQL","Flink"};
        List<JSONObject> products = new ArrayList<>();

        // 生产customer数据
        for(int i = 1;i <= 10; i++){

            JSONObject product = new JSONObject();

            Integer id = i;
            String name = names[new Random().nextInt(2)] + "-" + i;
            Integer price = new Random().nextInt(100);
            String category = "book";

            Long createTimestamp = new Random().nextInt(2678399 + 1) + 1577808000L; // 2020-01-01 00:00:00 ~ 2020-01-31 23:59:59
            String createTime = DateUtils.fromUnixtime(Long.valueOf(createTimestamp), DateUtils.DATE_TIME_PATTERN);

            product.put("id",id);
            product.put("name",name);
            product.put("price",price);
            product.put("category",category);
            product.put("create_time",createTime);

            products.add(product);

        }

        return products;

    }



    public static void generateStreamData(){

        customers = genCustomerData();
        products = genProductData();

        Integer customerNumber = customers.size();
        Integer productNumber = products.size();

        System.out.println("输出 " + customerNumber + " 个用户数据:");
        for(JSONObject customer :customers){
//            KafkaUtils.getInstance().send("gpman",customer.toJSONString());
            System.out.println(customer.toJSONString());
        }
        System.out.println("用户数据已全部输出.");

        System.out.println("输出 " + productNumber + " 个产品数据:");
        for(JSONObject product :products){
//            KafkaUtils.getInstance().send("gpman",product.toJSONString());
            System.out.println(product.toJSONString());
        }
        System.out.println("产品数据已全部输出.");


        /**
         * 生产 订单 数据
         */
        int id = 1; // 订单id
        int detailId = 1; // 订单商品id
        while (true){

            JSONObject order = new JSONObject();
            List<JSONObject> orderDetailList = new ArrayList<>();

            Integer customerId = new Random().nextInt(customerNumber) + 1;
            Integer orderStatus = new Random().nextInt(4) + 1;
            String createTime = null;
            try {

                if(new Random().nextInt(5) == 1){
                    createTime = DateUtils.timeAddOnSecond(new Date(),-10);
                }else{
                    createTime = DateUtils.dateFormat(new Date(), DateUtils.DATE_TIME_PATTERN);
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
            Integer orderAMT = 0;

//             订单明细数据
            List<Integer> productIds = new ArrayList<>();
            int productCountDistinct = new Random().nextInt(6) + 1;
            for(int i = 0;i < productCountDistinct; i++){

                JSONObject orderProduct = new JSONObject();

                Integer productId = new Random().nextInt(productNumber) + 1;
                while (productIds.contains(productId)){
                    productId = new Random().nextInt(productNumber) + 1;
                }
                productIds.add(productId);
                Integer productNum = new Random().nextInt(10) + 1;

                orderProduct.put("id",detailId);
                orderProduct.put("order_id",id);
                orderProduct.put("product_id",productId);
                orderProduct.put("product_num",productNum);
                orderProduct.put("create_time",createTime);

                orderDetailList.add(orderProduct);

                for(JSONObject product : products){

                    if(String.valueOf(productId).equals(product.getString("id"))){
                        orderAMT = orderAMT + Integer.valueOf(product.getString("price"));
                    }

                }

                detailId++;
            }

            // 父订单信息
            order.put("id",id);
            order.put("customer_id",customerId);
            order.put("order_amt",orderAMT);
            order.put("order_status",orderStatus);
            order.put("create_time",createTime);

            id++;

            // 数据订单数据
            KafkaUtils.getInstance().send(ConfigurationManager.getString("order.topics"),order.toJSONString());
            System.out.println("订单数据 ： " + order.toJSONString());

            for(JSONObject orderDetail :orderDetailList){
                KafkaUtils.getInstance().send(ConfigurationManager.getString("order.detail.topics"),orderDetail.toJSONString());
//                System.out.println("订单明细数据 ： " + orderDetail.toJSONString());
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }


    public static void generateBtachData(String baseDir){

    }


}
