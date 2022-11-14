package com.pgman.goku.mapper;

import java.util.ArrayList;
import java.util.List;

public class BookMapper {

    // 基础属性
    private int bookId;// 图书ID
    private String bookName; // 图书名称
    private double price; // 图书售价
    private int categoryId; // 品类ID
    private String categoryName; // 品类名称
    private String author; // 作者
    private String publisher; // 出版社
    private String publisherDate; // 出版日期
    private String createTime; // 创建时间

    // 保存商品列表
    public static List<BookMapper> books = new ArrayList<>();



    public BookMapper(int bookId, String bookName, double price, int categoryId, String categoryName, String author, String publisher, String publisherDate, String createTime) {
        this.bookId = bookId;
        this.bookName = bookName;
        this.price = price;
        this.categoryId = categoryId;
        this.categoryName = categoryName;
        this.author = author;
        this.publisher = publisher;
        this.publisherDate = publisherDate;
        this.createTime = createTime;
    }

    public int getBookId() {
        return bookId;
    }

    public void setBookId(int bookId) {
        this.bookId = bookId;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getPublisherDate() {
        return publisherDate;
    }

    public void setPublisherDate(String publisherDate) {
        this.publisherDate = publisherDate;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public static List<BookMapper> getBooks() {
        return books;
    }

    public static void setBooks(List<BookMapper> books) {
        BookMapper.books = books;
    }

    @Override
    public String toString() {
        return "BookMapper{" +
                "bookId=" + bookId +
                ", bookName='" + bookName + '\'' +
                ", price=" + price +
                ", categoryId=" + categoryId +
                ", categoryName='" + categoryName + '\'' +
                ", author='" + author + '\'' +
                ", publisher='" + publisher + '\'' +
                ", publisherDate='" + publisherDate + '\'' +
                ", createDataTime='" + createTime + '\'' +
                '}';
    }
}
