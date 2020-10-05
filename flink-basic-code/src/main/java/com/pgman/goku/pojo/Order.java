package com.pgman.goku.pojo;

/**
 * 订单实体类，用于封装数据使用
 */
public class Order{

    private Integer id;
    private Integer customerId;
    private Integer orderAmt;
    private Integer orderStatus;
    private String createTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Integer getOrderAmt() {
        return orderAmt;
    }

    public void setOrderAmt(Integer orderAmt) {
        this.orderAmt = orderAmt;
    }

    public Integer getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(Integer orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "order{" +
                "id=" + id +
                ", customerId=" + customerId +
                ", orderAmt=" + orderAmt +
                ", orderStatus=" + orderStatus +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}