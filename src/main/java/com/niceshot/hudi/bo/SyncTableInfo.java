package com.niceshot.hudi.bo;

import lombok.Builder;

import java.io.Serializable;

/**
 * @author created by chenjun at 2020-11-13 14:01
 */
public class SyncTableInfo implements Serializable {
    private String db;
    private String table;
    private String primaryKey;
    private String precombineKey;
    private String partitionKey;
    private String storeTable;
    private String realSavePath;

    public SyncTableInfo() {
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPrecombineKey() {
        return precombineKey;
    }

    public void setPrecombineKey(String precombineKey) {
        this.precombineKey = precombineKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getStoreTable() {
        return storeTable;
    }

    public void setStoreTable(String storeTable) {
        this.storeTable = storeTable;
    }

    public String getRealSavePath() {
        return realSavePath;
    }

    public void setRealSavePath(String realSavePath) {
        this.realSavePath = realSavePath;
    }


}
