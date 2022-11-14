package com.pgman.goku.mapper;

public class OrderMapper {

    private int orderId;
    private int shopId;
    private int userId;

    private double originalPrice;
    private double actualPrice;
    private double discountPrice;

    private String createTime;



    public OrderMapper(int orderId, int shopId, int userId, double originalPrice, double actualPrice, double discountPrice, String createTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.userId = userId;
        this.originalPrice = originalPrice;
        this.actualPrice = actualPrice;
        this.discountPrice = discountPrice;
        this.createTime = createTime;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public int getShopId() {
        return shopId;
    }

    public void setShopId(int shopId) {
        this.shopId = shopId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "OrderMapper{" +
                "orderId=" + orderId +
                ", shopId=" + shopId +
                ", userId=" + userId +
                ", originalPrice=" + originalPrice +
                ", actualPrice=" + actualPrice +
                ", discountPrice=" + discountPrice +
                ", createDateTime='" + createTime + '\'' +
                '}';
    }
}
