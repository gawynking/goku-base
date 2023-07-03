package com.pgman.goku.calcite.csv;


import com.pgman.goku.calcite.common.CdmColumn;
import com.pgman.goku.calcite.common.CdmTable;


import java.util.List;

public class CsvTable extends CdmTable {
    /**
     * 数据路径
     */
    private String dataPath;

    public CsvTable(String name, List<CdmColumn> columns, String dataPath) {
        super(name, columns);
        this.dataPath = dataPath;
    }


    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }
}
