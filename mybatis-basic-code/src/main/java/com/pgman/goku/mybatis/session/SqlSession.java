package com.pgman.goku.mybatis.session;

public interface SqlSession {

    <T> T getMapper(Class<T> daoInterfaceClass);

    void close();

}
