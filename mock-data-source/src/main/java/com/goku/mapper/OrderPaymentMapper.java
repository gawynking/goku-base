package com.goku.mapper;

public class OrderPaymentMapper {

    private int payId;
    private int orderId;

    private double paymentAmount;

    private double paymentStatus;

    private String createTime;

    public OrderPaymentMapper(int payId, int orderId, double paymentAmount, double paymentStatus, String createTime) {
        this.payId = payId;
        this.orderId = orderId;
        this.paymentAmount = paymentAmount;
        this.paymentStatus = paymentStatus;
        this.createTime = createTime;
    }

    public int getPayId() {
        return payId;
    }

    public void setPayId(int payId) {
        this.payId = payId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPaymentAmount() {
        return paymentAmount;
    }

    public void setPaymentAmount(double paymentAmount) {
        this.paymentAmount = paymentAmount;
    }

    public double getPaymentStatus() {
        return paymentStatus;
    }

    public void setPaymentStatus(double paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "OrderPayment{" +
                "payId=" + payId +
                ", orderId=" + orderId +
                ", paymentAmount=" + paymentAmount +
                ", paymentStatus=" + paymentStatus +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
