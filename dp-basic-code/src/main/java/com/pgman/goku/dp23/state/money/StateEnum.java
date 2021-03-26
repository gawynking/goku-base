package com.pgman.goku.dp23.state.money;

public enum StateEnum {

    GENERATE(1, "GENERATE"),

    REVIEWED(2, "REVIEWED"),

    PUBLISHED(3, "PUBLISHED"),

    NOT_PAY(4, "NOT_PAY"),

    PAID(5, "PAID"),

    FEED_BACKED(6, "FEED_BACKED");

    private int key;
    private String value;

    StateEnum(int key, String value) {
        this.key = key;
        this.value = value;
    }

    public int getKey() {return key;}
    public String getValue() {return value;}

}
