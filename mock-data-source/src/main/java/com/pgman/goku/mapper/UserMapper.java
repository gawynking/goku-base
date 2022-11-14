package com.pgman.goku.mapper;

import java.util.ArrayList;
import java.util.List;

public class UserMapper {

    private int userId;
    private String userName;
    private int sex;
    private String account;
    private String nickName;
    private String registerTime;
    private int cityId;
    private String cityName;
    private String crowdType;

    public static List<UserMapper> users = new ArrayList<>();



    public UserMapper(int userId, String userName, int sex, String account, String nickName, String registerTime, int cityId, String cityName, String crowdType) {
        this.userId = userId;
        this.userName = userName;
        this.sex = sex;
        this.account = account;
        this.nickName = nickName;
        this.registerTime = registerTime;
        this.cityId = cityId;
        this.cityName = cityName;
        this.crowdType = crowdType;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(String registerTime) {
        this.registerTime = registerTime;
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

    public String getCrowdType() {
        return crowdType;
    }

    public void setCrowdType(String crowdType) {
        this.crowdType = crowdType;
    }

    public static List<UserMapper> getUsers() {
        return users;
    }

    public static void setUsers(List<UserMapper> users) {
        UserMapper.users = users;
    }

    @Override
    public String toString() {
        return "UserMapper{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", sex=" + sex +
                ", account='" + account + '\'' +
                ", nickName='" + nickName + '\'' +
                ", registerDateTime='" + registerTime + '\'' +
                ", cityId=" + cityId +
                ", cityName='" + cityName + '\'' +
                ", crowdType='" + crowdType + '\'' +
                '}';
    }
}
