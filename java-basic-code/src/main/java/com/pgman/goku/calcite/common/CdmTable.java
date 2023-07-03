package com.pgman.goku.calcite.common;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class CdmTable extends AbstractTable {
    /**
     * 表名
     */
    private String name;
    /**
     * 表的列
     */
    private List<CdmColumn> columns;

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();

        for (CdmColumn c : columns) {
            names.add(c.getName());
            RelDataType sqlType = typeFactory.createSqlType(SqlTypeName.get(c.getType().toUpperCase()));
            types.add(sqlType);
        }
        return typeFactory.createStructType(Pair.zip(names, types));

    }


    public CdmTable(String name, List<CdmColumn> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CdmColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<CdmColumn> columns) {
        this.columns = columns;
    }
}
