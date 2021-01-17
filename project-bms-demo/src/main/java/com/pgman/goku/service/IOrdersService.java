package com.pgman.goku.service;

import com.pgman.goku.domain.Orders;

import java.util.List;

public interface IOrdersService {

    List<Orders> findAll(int page, int size) throws Exception;

    Orders findById(String ordersId) throws Exception;

}
