package com.pgman.travel.dao;

import com.pgman.travel.domain.RouteImg;

import java.util.List;

public interface RouteImgDao {

    /**
     * 根据route的id查询图片
     *
     * @param rid
     * @return
     */
    public List<RouteImg> findByRid(int rid);


}
