package com.pgman.goku.factory;

import com.pgman.goku.service.IDeptService;
import com.pgman.goku.service.impl.DeptServiceXmlImpl;

public class InstanceFactory {

    public IDeptService getDeptService(){
        return new DeptServiceXmlImpl();
    }
}
