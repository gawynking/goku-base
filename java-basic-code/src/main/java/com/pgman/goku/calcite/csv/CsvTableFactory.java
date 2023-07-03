package com.pgman.goku.calcite.csv;

import com.fasterxml.jackson.core.type.TypeReference;
import com.pgman.goku.calcite.common.CdmColumn;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.pgman.goku.util.JSONUtils.JSON_MAPPER;


public class CsvTableFactory implements TableFactory<CsvTable> {

    public CsvTable create(
            SchemaPlus schema,
            String name,
            Map operand,
            RelDataType rowType
    ) {
        //schema.add("test", ScalarFunctionImpl.create(MyFunction.class, "test"));
        final String colTypePath = String.valueOf(operand.get("colPath"));
        final List<CdmColumn> columns;
        try {
            columns = JSON_MAPPER.readValue(
                        new File(colTypePath),
                        new TypeReference<List<CdmColumn>>() {}
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CsvTable(name, columns, String.valueOf(operand.get("dataPath")));
    }
}
