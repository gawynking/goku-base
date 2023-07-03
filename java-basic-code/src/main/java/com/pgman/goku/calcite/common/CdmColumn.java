package com.pgman.goku.calcite.common;


import org.apache.calcite.sql.type.SqlTypeName;

public class CdmColumn {
    /**
     * 列名
     */
    private String name;
    /**
     * 列类型,可以使用calcite扩展的sql类型：{@link SqlTypeName}
     */
    private String type;

    public CdmColumn() {}
    public CdmColumn(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }
}
