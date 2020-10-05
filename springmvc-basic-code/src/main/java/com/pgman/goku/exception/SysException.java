package com.pgman.goku.exception;

public class SysException extends Exception {

    private String message;

    public SysException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
