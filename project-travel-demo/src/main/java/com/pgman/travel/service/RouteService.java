package com.pgman.travel.service;

import com.pgman.travel.domain.PageBean;
import com.pgman.travel.domain.Route;

/**
 * 线路Service
 */
public interface RouteService {

    /**
     * 根据类别进行分页查询
     *
     * @param cid
     * @param currentPage
     * @param pageSize
     * @return
     */
    public PageBean<Route> pageQuery(int cid, int currentPage, int pageSize,String rname);

    /**
     * 根据id查询
     *
     * @param rid
     * @return
     */
    public Route findOne(String rid);

}
