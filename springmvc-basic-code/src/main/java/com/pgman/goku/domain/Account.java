package com.pgman.goku.domain;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Account implements Serializable{

    private String username;
    private String password;
    private int money;

    private User user;

    private List<User> users;
    private Map<String,User> maps;

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public Map<String, User> getMaps() {
        return maps;
    }

    public void setMaps(Map<String, User> maps) {
        this.maps = maps;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMoney() {
        return money;
    }

    public void setMoney(int money) {
        this.money = money;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "Account{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", money=" + money +
                ", user=" + user +
                ", users=" + users +
                ", maps=" + maps +
                '}';
    }
}
