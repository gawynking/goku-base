package com.pgman.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.mapper.BookShopMapper;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.KafkaUtils;
import com.pgman.goku.util.ObjectUtils;

import java.util.*;

public class MockBookShopData {

    public static boolean cycleFlag = true;
    private static boolean testFlag = false;

    public static boolean isCycleFlag() {
        return cycleFlag;
    }

    public static void setCycleFlag(boolean cycleFlag) {
        MockBookShopData.cycleFlag = cycleFlag;
    }

    public static void mockBookShop(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        Random random = new Random();

        Map<Integer, String> bookShopLevelPool = new HashMap<>();
        bookShopLevelPool.put(1, "S级");
        bookShopLevelPool.put(2, "A级");
        bookShopLevelPool.put(3, "B级");
        bookShopLevelPool.put(4, "C级");

        Map<Integer, String> bookShopCityPool = new HashMap<>();
        bookShopCityPool.put(1, "北京市");
        bookShopCityPool.put(2, "上海市");
        bookShopCityPool.put(3, "广州市");

        Map<Integer, String> bookShopBrandPool = new HashMap<>();
        bookShopBrandPool.put(1, "亚马逊书店");
        bookShopBrandPool.put(2, "当当书店");
        bookShopBrandPool.put(3, "京东书店");
        bookShopBrandPool.put(4, "淘宝书店");
        bookShopBrandPool.put(5, "美团书店");
        bookShopBrandPool.put(6, "字节书店");
        bookShopBrandPool.put(7, "滴滴书店");
        bookShopBrandPool.put(8, "拼多多书店");
        bookShopBrandPool.put(9, "阿里书店");

        Map<Integer, String[]> bookShopManagerPool = new HashMap<>();
        for (int i = 1; i <= bookShopBrandPool.size() * bookShopCityPool.size(); i++) {
            Integer shopManagerId = i;
            String shopManagerName = "书店经理-" + i;
            String brandId = String.valueOf(random.nextInt(bookShopBrandPool.size()) + 1);
            bookShopManagerPool.put(shopManagerId, new String[]{shopManagerName, brandId});
        }


        List<BookShopMapper> bookShops = BookShopMapper.bookShops;

        int shopId = 0;
        String shopName;
        int cityId;
        String cityName;
        String shopAddress;
        Integer levelId;
        String levelName;

        String openDate;
        int brandId;
        String brandName;
        int managerId;
        String managerName;
        String createDataTime;


        List<BookShopMapper> cache = new ArrayList<>(unOrderedNum);
        int i = 0;
        while (cycleFlag) {

            shopId = shopId + 1;
            shopName = "网上书店-" + shopId;
            cityId = random.nextInt(bookShopCityPool.size()) + 1;
            cityName = bookShopCityPool.get(cityId);
            shopAddress = "地址:" + cityName + "-" + String.valueOf(random.nextInt(9999)) + "-号";
            levelId = random.nextInt(bookShopLevelPool.size()) + 1;
            levelName = bookShopLevelPool.get(levelId);
            openDate = DateUtils.getCurrentDate();
            managerId = random.nextInt(bookShopManagerPool.size()) + 1;
            managerName = bookShopManagerPool.get(managerId)[0];
            brandId = Integer.valueOf(bookShopManagerPool.get(managerId)[1]);
            brandName = bookShopBrandPool.get(brandId);
            createDataTime = DateUtils.getCurrentDatetime();

            BookShopMapper bookShop = new BookShopMapper(
                    shopId,
                    shopName,
                    shopAddress,
                    levelId,
                    levelName,
                    cityId,
                    cityName,
                    openDate,
                    brandId,
                    brandName,
                    managerId,
                    managerName,
                    createDataTime

            );
            bookShops.add(bookShop);

            if (i < unOrderedNum) {
                cache.add(bookShop);
                i++;
            } else {
                i = 0;
                if (orderedFlag) {
                    for (BookShopMapper entry : cache) {
                        if(testFlag){
                            System.out.println(entry.toString());
                        }else {
                            JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookShopMapper.class);
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("shop.topics"), jsonObject.toString());
                        }
                    }
                } else {
                    Collections.shuffle(cache);
                    for (BookShopMapper entry : cache) {
                        if(testFlag){
                            System.out.println(entry.toString());
                        }else {
                            JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookShopMapper.class);
                            KafkaUtils.getInstance().send(ConfigurationManager.getString("shop.topics"), jsonObject.toString());
                        }
                    }
                }
                cache.clear();
                cache.add(bookShop);
                i++;
            }
            Thread.sleep(random.nextInt(sleepTime));
        }

        for (BookShopMapper entry : cache) {
            if(testFlag){
                System.out.println(entry.toString());
            }else {
                JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookShopMapper.class);
                KafkaUtils.getInstance().send(ConfigurationManager.getString("shop.topics"), jsonObject.toString());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        MockBookShopData.mockBookShop(false, 1000, 3);
    }

}
