package com.goku.doc.table;

public class UnimplementedException extends RuntimeException {
    public UnimplementedException() {
        super("This method has not yet been implemented");
    }
}