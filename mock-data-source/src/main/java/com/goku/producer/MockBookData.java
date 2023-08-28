package com.goku.producer;

import com.alibaba.fastjson.JSONObject;
import com.goku.config.ConfigurationManager;
import com.goku.mapper.BookMapper;
import com.goku.util.DateUtils;
import com.goku.util.KafkaUtils;
import com.goku.util.MysqlJDBCUtils;
import com.goku.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.*;

public class MockBookData {

    private static String countSQL = "select count(1) as cnt from tbl_book where book_id = ?";

    private static String insertSQL = "insert into tbl_book(\n" +
            "    book_id,\n" +
            "    book_name,\n" +
            "    price,\n" +
            "    category_id,\n" +
            "    category_name,\n" +
            "    author,\n" +
            "    publisher,\n" +
            "    publisher_date,\n" +
            "    create_time\n" +
            ")value(\n" +
            "    ?,\n" +
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
    private static boolean testFlag = false;

    public static boolean isCycleFlag() {
        return cycleFlag;
    }

    public static void setCycleFlag(boolean cycleFlag) {
        MockBookData.cycleFlag = cycleFlag;
    }

    public static void mockBook(boolean orderedFlag, int sleepTime, int unOrderedNum) throws InterruptedException {

        Random random = new Random();

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
        category.put(10, "Hudi");
        category.put(11, "Scala");
        category.put(12, "Doris");

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
            categoryId = random.nextInt(category.size())+1;
            categoryName = category.get(categoryId);
            bookName = categoryName + "-" + String.valueOf(random.nextInt(9999)) + "-版";
            price = new BigDecimal(random.nextDouble() * 100).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
            author = categoryName + "-author:" + String.valueOf(random.nextInt(9999));
            publisher = categoryName + "-publisher:" + String.valueOf(random.nextInt(9999));
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
                if(!orderedFlag){
                    Collections.shuffle(cache);
                }

                for(BookMapper entry :cache){
                    if(testFlag){
                        System.out.println(entry.toString());
                    }else {
                        JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookMapper.class);

//                        将数据写入外部存储
                        if(!(MysqlJDBCUtils.getInstance().getCount(countSQL,new Object[]{entry.getBookId()}) > 0)) {
                            MysqlJDBCUtils.getInstance().executeUpdate(
                                    insertSQL,
                                    new Object[]{
                                            entry.getBookId(),
                                            entry.getBookName(),
                                            entry.getPrice(),
                                            entry.getCategoryId(),
                                            entry.getCategoryName(),
                                            entry.getAuthor(),
                                            entry.getPublisher(),
                                            entry.getPublisher(),
                                            entry.getCreateTime()
                                    }
                            );
                        }
                        KafkaUtils.getInstance().send(ConfigurationManager.getString("book.topics"), jsonObject.toString());
                    }
                }

                cache.clear();
                cache.add(book);
                i++;
            }
            Thread.sleep(random.nextInt(sleepTime));
        }

        for(BookMapper entry :cache){
            if(testFlag){
                System.out.println(entry.toString());
            }else {
                JSONObject jsonObject = ObjectUtils.objInstanceToJsonObject(entry, BookMapper.class);

//                        将数据写入外部存储
                if(!(MysqlJDBCUtils.getInstance().getCount(countSQL,new Object[]{entry.getBookId()}) > 0)) {
                    MysqlJDBCUtils.getInstance().executeUpdate(
                            insertSQL,
                            new Object[]{
                                    entry.getBookId(),
                                    entry.getBookName(),
                                    entry.getPrice(),
                                    entry.getCategoryId(),
                                    entry.getCategoryName(),
                                    entry.getAuthor(),
                                    entry.getPublisher(),
                                    entry.getPublisher(),
                                    entry.getCreateTime()
                            }
                    );
                }
                KafkaUtils.getInstance().send(ConfigurationManager.getString("book.topics"), jsonObject.toString());
            }
        }

    }

    public static void main(String[] args) throws Exception{
        testFlag = false;
        new MockBookData().mockBook(false,1000,3);
    }

}
