package com.pgman.goku.factory;

import com.pgman.goku.service.IDeptService;
import com.pgman.goku.service.impl.DeptServiceXmlImpl;

public class StaticFactory {

    public static IDeptService getDeptService(){
        return new DeptServiceXmlImpl();
    }
}
