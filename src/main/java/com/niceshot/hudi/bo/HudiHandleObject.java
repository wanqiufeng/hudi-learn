package com.niceshot.hudi.bo;


import java.util.List;

/**
 * @author created by chenjun at 2020-10-23 18:35
 */
public class HudiHandleObject {
    private String database;
    private String table;
    private String operationType;
    private List<String> data;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }
}
