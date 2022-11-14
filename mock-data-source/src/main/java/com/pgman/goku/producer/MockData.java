package com.pgman.goku.producer;

import com.pgman.goku.mapper.BookMapper;
import com.pgman.goku.mapper.BookShopMapper;
import com.pgman.goku.mapper.UserMapper;

import java.util.List;

public class MockData {

    private static int bookShopNumber = 100;
    private static int bookNumber = 1000;
    private static int userNumber = 100;

    // 设置维度池限额
    public static void setNumber(int bookShopNumber,int bookNumber,int userNumber){
        MockData.bookShopNumber = bookShopNumber;
        MockData.bookNumber = bookNumber;
        MockData.userNumber = userNumber;
    }

    public static void mockData(boolean orderedFlag,int sleepTime,int unOrderedNum){

        List<BookShopMapper> bookShops = BookShopMapper.bookShops;
        List<BookMapper> books = BookMapper.books;
        List<UserMapper> users = UserMapper.users;

        // 监控线程
        new Thread(new Runnable() {
            @Override
            public void run() {
                boolean flag = true;
                while (flag){
                    try {
                        Thread.sleep(100);

                        if(bookShops.size()>=bookShopNumber && MockBookShopData.isCycleFlag()){
                            MockBookShopData.setCycleFlag(false);
                            System.out.println("===> Thread MockBookShopData stop!");
                        }
                        if(books.size()>=bookNumber && MockBookData.isCycleFlag()){
                            MockBookData.setCycleFlag(false);
                            System.out.println("===> Thread MockBookData stop!");
                        }
                        if(users.size()>=userNumber && MockUserData.isCycleFlag()){
                            MockUserData.setCycleFlag(false);
                            System.out.println("===> Thread MockUserData stop!");
                        }
                        if(!(MockBookShopData.isCycleFlag() || MockBookData.isCycleFlag() || MockUserData.isCycleFlag())){
                            flag = false;
                            System.out.println("===> Threads Monitor && MockBookShopData && MockBookData && MockUserData stop!");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();


        // 生产BookShop
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    new MockBookShopData().mockBookShop(orderedFlag,sleepTime,unOrderedNum);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // 生产Book
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    new MockBookData().mockBook(orderedFlag,sleepTime,unOrderedNum);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // 生产User
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    new MockUserData().mockUser(orderedFlag,sleepTime,unOrderedNum);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // 生产Order
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(sleepTime*5);
                    new MockOrderData().mockOrder(orderedFlag,sleepTime,unOrderedNum);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

    }


    public static void main(String[] args) {
        setNumber(10,20,5);
        new MockData().mockData(true,1000,3);
    }

}
