package com.niceshot.hudi.bo;


import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author created by chenjun at 2020-10-23 18:35
 */
public class HudiHandleObject {
    private String database;
    private String table;
    private String operationType;
    private List<Map<String,String>> data;
    private List<String> jsonData;


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

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public List<String> getJsonData() {
        return jsonData;
    }

    public void setJsonData(List<String> jsonData) {
        this.jsonData = jsonData;
    }
}
