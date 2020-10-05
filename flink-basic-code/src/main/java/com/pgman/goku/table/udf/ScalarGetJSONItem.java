package com.pgman.goku.table.udf;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.ScalarFunction;

public class ScalarGetJSONItem extends ScalarFunction{

    public ScalarGetJSONItem(){

    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return super.getResultType(signature);
    }

    public String eval(String json,String keyStr){

        try {
            return JSONUtils.getJSONField(JSONUtils.getJSONObjectFromString(json),keyStr);
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;

    }

}
