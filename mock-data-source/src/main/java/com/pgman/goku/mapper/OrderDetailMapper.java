package com.pgman.goku.mapper;

public class OrderDetailMapper {

    private int orderBookId;
    private int orderId;
    private int bookId;

    private double originalPrice;
    private double actualPrice;
    private double discountPrice;

    private int bookNumber;

    private String createTime;



    public OrderDetailMapper(int orderBookId, int orderId, int bookId, double originalPrice, double actualPrice, double discountPrice, int bookNumber, String createTime) {
        this.orderBookId = orderBookId;
        this.orderId = orderId;
        this.bookId = bookId;
        this.originalPrice = originalPrice;
        this.actualPrice = actualPrice;
        this.discountPrice = discountPrice;
        this.bookNumber = bookNumber;
        this.createTime = createTime;
    }

    public int getOrderBookId() {
        return orderBookId;
    }

    public void setOrderBookId(int orderBookId) {
        this.orderBookId = orderBookId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public int getBookId() {
        return bookId;
    }

    public void setBookId(int bookId) {
        this.bookId = bookId;
    }

    public double getOriginalPrice() {
        return originalPrice;
    }

    public void setOriginalPrice(double originalPrice) {
        this.originalPrice = originalPrice;
    }

    public double getActualPrice() {
        return actualPrice;
    }

    public void setActualPrice(double actualPrice) {
        this.actualPrice = actualPrice;
    }

    public double getDiscountPrice() {
        return discountPrice;
    }

    public void setDiscountPrice(double discountPrice) {
        this.discountPrice = discountPrice;
    }

    public int getBookNumber() {
        return bookNumber;
    }

    public void setBookNumber(int bookNumber) {
        this.bookNumber = bookNumber;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "OrderDetailMapper{" +
                "orderBookId=" + orderBookId +
                ", orderId=" + orderId +
                ", bookId=" + bookId +
                ", originalPrice=" + originalPrice +
                ", actualPrice=" + actualPrice +
                ", discountPrice=" + discountPrice +
                ", bookNumber=" + bookNumber +
                ", createDateTime='" + createTime + '\'' +
                '}';
    }
}
