package com.pgman.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.mapper.BookMapper;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.KafkaUtils;
import com.pgman.goku.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.*;

public class MockBookData {

    public static boolean cycleFlag = true;

    public static boolean isCycleFlag() {
        return cycleFlag;
    }

    public static void setCycleFlag(boolean cycleFlag) {
        MockBookData.cycleFlag = cycleFlag;
    }

    public static void mockBook(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        HashMap<Integer, String> category = new HashMap<>();
        category.put(1, "Java");
        category.put(2, "SQL");
        category.put(3, "Python");
        category.put(4, "Oracle");
        category.put(5, "MySQL");
        category.put(6, "PostgreSQL");
        category.put(7, "Flink");
        category.put(8, "Spark");
        category.put(9, "Kafka");

        List<BookMapper> books = BookMapper.books;

        int bookId = 0;
        int categoryId;
        String categoryName;
        String bookName;
        double price;
        String author;
        String publisher;
        String publisherDate;
        String createDataTime;

        List<BookMapper> cache = new ArrayList<>(unOrderedNum);
        int i = 0;
        while (cycleFlag) {

            bookId = bookId + 1;
            categoryId = new Random().nextInt(category.size())+1;
            categoryName = category.get(categoryId);
            bookName = categoryName + "-" + String.valueOf(new Random().nextInt(9999)) + "-版";
            price = new BigDecimal(new Random().nextDouble() * 100).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
            author = categoryName + "-author:" + String.valueOf(new Random().nextInt(9999));
            publisher = categoryName + "-publisher:" + String.valueOf(new Random().nextInt(9999));
            publisherDate = DateUtils.getCurrentDate();
            createDataTime = DateUtils.getCurrentDatetime();

            BookMapper book = new BookMapper(
                    bookId,
                    bookName,
                    price,
                    categoryId,
                    categoryName,
                    author,
                    publisher,
                    publisherDate,
                    createDataTime
            );
            books.add(book);

            if(i<unOrderedNum){
                cache.add(book);
                i++;
            }else {
                i=0;
                if(orderedFlag){
                    for(BookMapper entry :cache){
                        JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookMapper.class);
//                        System.out.println(jsonObject.toString());
                        KafkaUtils.getInstance().send(ConfigurationManager.getString("book.topics"),jsonObject.toString());
                        Thread.sleep(new Random().nextInt(sleepTime));
                    }
                }else {
                    cache.sort(new Comparator<BookMapper>() {
                        @Override
                        public int compare(BookMapper o1, BookMapper o2) {
                            return new Random().nextInt(9) - new Random().nextInt(9);
                        }
                    });
                    for(BookMapper entry :cache){
                        JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookMapper.class);
//                        System.out.println(jsonObject.toString());
                        KafkaUtils.getInstance().send(ConfigurationManager.getString("book.topics"),jsonObject.toString());
                        Thread.sleep(new Random().nextInt(sleepTime));
                    }
                }
                cache.clear();
                cache.add(book);
                i++;
            }

        }

        for(BookMapper entry :cache){
            JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookMapper.class);
//            System.out.println(jsonObject.toString());
            KafkaUtils.getInstance().send(ConfigurationManager.getString("book.topics"),jsonObject.toString());
            Thread.sleep(new Random().nextInt(sleepTime));
        }

    }

    public static void main(String[] args) throws Exception{
        new MockBookData().mockBook(false,1000,3);
    }

}
