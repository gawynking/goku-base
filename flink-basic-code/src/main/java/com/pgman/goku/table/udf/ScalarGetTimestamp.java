package com.pgman.goku.table.udf;

import com.pgman.goku.util.DateUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class ScalarGetTimestamp extends ScalarFunction{

    public ScalarGetTimestamp(){

    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP();
    }

    public Long eval(String dateStr){

        try {
            return DateUtils.getTimestamp(dateStr,"yyyy-MM-dd HH:mm:ss");
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;

    }

}
