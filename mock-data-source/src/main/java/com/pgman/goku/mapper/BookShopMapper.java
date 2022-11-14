package com.pgman.goku.mapper;

import java.util.*;

public class BookShopMapper {

    // 基础属性
    private int shopId; // 书店ID
    private String shopName; // 书店名称
    private String shopAddress; // 书店地址

    private int levelId; // 书店等级
    private String levelName; // 书店等级名称

    private int cityId; // 城市ID
    private String cityName; // 城市名称

    private String openDate; // 开店日期

    private int brandId; // 品牌ID
    private String brandName; // 品牌名称
    private int managerId; // 经理ID
    private String managerName; // 经理名称

    private String createTime; // 创建时间


    // 存储门店列表
    public static List<BookShopMapper> bookShops = new ArrayList<>();



    public BookShopMapper(int shopId, String shopName, String shopAddress, int levelId, String levelName, int cityId, String cityName, String openDate, int brandId, String brandName, int managerId, String managerName, String createTime) {
        this.shopId = shopId;
        this.shopName = shopName;
        this.shopAddress = shopAddress;
        this.levelId = levelId;
        this.levelName = levelName;
        this.cityId = cityId;
        this.cityName = cityName;
        this.openDate = openDate;
        this.brandId = brandId;
        this.brandName = brandName;
        this.managerId = managerId;
        this.managerName = managerName;
        this.createTime = createTime;
    }

    public int getShopId() {
        return shopId;
    }

    public void setShopId(int shopId) {
        this.shopId = shopId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public String getShopAddress() {
        return shopAddress;
    }

    public void setShopAddress(String shopAddress) {
        this.shopAddress = shopAddress;
    }

    public int getLevelId() {
        return levelId;
    }

    public void setLevelId(int levelId) {
        this.levelId = levelId;
    }

    public String getLevelName() {
        return levelName;
    }

    public void setLevelName(String levelName) {
        this.levelName = levelName;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getOpenDate() {
        return openDate;
    }

    public void setOpenDate(String openDate) {
        this.openDate = openDate;
    }

    public int getBrandId() {
        return brandId;
    }

    public void setBrandId(int brandId) {
        this.brandId = brandId;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public int getManagerId() {
        return managerId;
    }

    public void setManagerId(int managerId) {
        this.managerId = managerId;
    }

    public String getManagerName() {
        return managerName;
    }

    public void setManagerName(String managerName) {
        this.managerName = managerName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public static List<BookShopMapper> getBookShops() {
        return bookShops;
    }

    public static void setBookShops(List<BookShopMapper> bookShops) {
        BookShopMapper.bookShops = bookShops;
    }

    @Override
    public String toString() {
        return "BookShopMapper{" +
                "shopId=" + shopId +
                ", shopName='" + shopName + '\'' +
                ", shopAddress='" + shopAddress + '\'' +
                ", levelId=" + levelId +
                ", levelName='" + levelName + '\'' +
                ", cityId=" + cityId +
                ", cityName='" + cityName + '\'' +
                ", openDate='" + openDate + '\'' +
                ", brandId=" + brandId +
                ", brandName='" + brandName + '\'' +
                ", managerId=" + managerId +
                ", managerName='" + managerName + '\'' +
                ", createDataTime='" + createTime + '\'' +
                '}';
    }
}
