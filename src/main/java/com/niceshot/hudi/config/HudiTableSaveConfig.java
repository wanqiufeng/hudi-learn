package com.niceshot.hudi.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * @author created by chenjun at 2020-11-02 14:33
 */
public class HudiTableSaveConfig implements Serializable {
    //to-do:mark 添加非空约束
    @Parameter(names = {"--base-save-path"},description = "base data file path for hudi table store, has no default value.eg:hdfs://192.168.16.181:8020/hudi_data/")
    private String baseSavePath;
    @Parameter(names = {"--real-save-path"},description = "table data real store path,suggest to use default value. " +
            " default value is  construct by --base-save-path,--db-name,--table-name." +
            "eg:--base-save-path='hdfs://192.168.16.181:8020/hudi_data/',--db-name='crm',--table-name='order',then real save path is :hdfs://192.168.16.181:8020/hudi_data/crm__order")
    private String realSavePath;
    @Parameter(names = {"--mapping-mysql-db-name"},description = "mysql db which binlog will handle in current application,has no default value. eg. crm")
    private String mappingMysqlDbName;
    @Parameter(names = {"--mapping-mysql-table-name"},description = "mysql table which binlog will handle in current application,has no default value. eg. 'order'")
    private String mappingMysqlTableName;
    @Parameter(names = {"--store-table-name"},description = "stored hudi name,suggest use default value.default value is --sync-db-name concact --sync-table-name." +
            "eg:--sync-db-name is crm , --sync-table-name is order ,then table name is 'crm__order'")
    private String storeTableName;
    @Parameter(names = {"--primary-key"},description = "hive table's primary key ,default value is 'id'")
    private String primaryKey="id";
    @Parameter(names = {"--partition-key"},description = "hive table field used for hudi partition,has no default value,prefer timestamp or dateime field.eg:'create_date'")
    private String partitionKey;
    @Parameter(names = {"--precombine-key"},description = "use for hudi config 'hoodie.datasource.write.precombine.field'.default value is 'id'")
    private String precombineKey = "id";
    @Parameter(names = {"--hive-site-path"},description = "hive-site.xml path")
    private String hiveConfFilePath;

    public String getBaseSavePath() {
        return baseSavePath;
    }

    public void setBaseSavePath(String baseSavePath) {
        this.baseSavePath = baseSavePath;
    }

    public String getRealSavePath() {
        return realSavePath;
    }

    public void setRealSavePath(String realSavePath) {
        this.realSavePath = realSavePath;
    }

    public String getMappingMysqlDbName() {
        return mappingMysqlDbName;
    }

    public void setMappingMysqlDbName(String mappingMysqlDbName) {
        this.mappingMysqlDbName = mappingMysqlDbName;
    }

    public String getMappingMysqlTableName() {
        return mappingMysqlTableName;
    }

    public void setMappingMysqlTableName(String mappingMysqlTableName) {
        this.mappingMysqlTableName = mappingMysqlTableName;
    }

    public String getStoreTableName() {
        return storeTableName;
    }

    public void setStoreTableName(String storeTableName) {
        this.storeTableName = storeTableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getPrecombineKey() {
        return precombineKey;
    }

    public void setPrecombineKey(String precombineKey) {
        this.precombineKey = precombineKey;
    }

    public String getHiveConfFilePath() {
        return hiveConfFilePath;
    }

    public void setHiveConfFilePath(String hiveConfFilePath) {
        this.hiveConfFilePath = hiveConfFilePath;
    }
}
