package com.pgman.goku.pojo;

public class OrderList {

    private Integer id;
    private Integer orderId;
    private Integer productId;
    private Integer productNum;
    private String createTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public Integer getProductNum() {
        return productNum;
    }

    public void setProductNum(Integer productNum) {
        this.productNum = productNum;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "OrderList{" +
                "id=" + id +
                ", orderId=" + orderId +
                ", productId=" + productId +
                ", productNum=" + productNum +
                ", createTime='" + createTime + '\'' +
                '}';
    }

}
