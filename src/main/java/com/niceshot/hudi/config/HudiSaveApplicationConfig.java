package com.niceshot.hudi.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

/**
 * @author created by chenjun at 2020-10-30 14:01
 */
public class HudiSaveApplicationConfig {
    @Parameter(names = {"--base-save-path"},description = "base data file path for hudi table store, has no default value.eg:hdfs://192.168.16.181:8020/hudi_data/")
    private String baseSavePath;
    @Parameter(names = {"--real-save-path"},description = "table data real store path,suggest to use default value. " +
            " default value is  construct by --base-save-path,--db-name,--table-name." +
            "eg:--base-save-path='hdfs://192.168.16.181:8020/hudi_data/',--db-name='crm',--table-name='order',then real save path is :hdfs://192.168.16.181:8020/hudi_data/crm__order")
    private String realSavePath;
    @Parameter(names = {"--sync-db-name"},description = "mysql db which binlog will handle in current application,has no default value. eg. crm")
    private String syncDbName;
    @Parameter(names = {"--sync-table-name"},description = "mysql table which binlog will handle in current application,has no default value. eg. 'order'")
    private String syncTableName;
    @Parameter(names = {"--store-table-name"},description = "stored hudi name,suggest use default value.default value is --sync-db-name concact --sync-table-name." +
            "eg:--sync-db-name is crm , --sync-table-name is order ,then table name is 'crm__order'")
    private String storeTableName;
    @Parameter(names = {"--kafka-server"},description = "kafka server which stored binlog data from alibaba canal, has no default value . " +
            "eg: 192.168.16.237:9092,192.168.16.236:9092")
    private String kafkaServer;
    @Parameter(names = {"--kafka-topic"},description = "kafka topic which for consume binlog from canal,has no default value.")
    private String kafkaTopic;
    @Parameter(names = {"--kafka-group"},description = "kafka group which for consume binlog from canal,suggest use default value ." +
            "default value construct by dbName ,tableName.eg. dbName:crm ,tableName:order, then topic name is :hudi_crm__order")
    private String kafkaGroup;
    @Parameter(names = {"--primary-key"},description = "mysql table's primary key ,default value is 'id'")
    private String primaryKey="id";
    @Parameter(names = {"--create-time-stamp-key"},description = "mysql table filed used for hudi partition,has no default value.eg:'create_date'")
    private String createTimeStampKey;
    @Parameter(names = {"--precombine-key"},description = "use for hudi config 'hoodie.datasource.write.precombine.field'.default value is 'id'")
    private String precombineKey = "id";
    @Parameter(names = {"--duration-seconds"},description = "batch time length for spark streaming,default is '20'")
    private Long durationSeconds = 10L;



    public String getKafkaServer() {
        return kafkaServer;
    }

    public void setKafkaServer(String kafkaServer) {
        this.kafkaServer = kafkaServer;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaGroup() {
        return kafkaGroup;
    }

    public void setKafkaGroup(String kafkaGroup) {
        this.kafkaGroup = kafkaGroup;
    }


    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getCreateTimeStampKey() {
        return createTimeStampKey;
    }

    public void setCreateTimeStampKey(String createTimeStampKey) {
        this.createTimeStampKey = createTimeStampKey;
    }

    public String getPrecombineKey() {
        return precombineKey;
    }

    public void setPrecombineKey(String precombineKey) {
        this.precombineKey = precombineKey;
    }

    public Long getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(Long durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

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

    public String getSyncDbName() {
        return syncDbName;
    }

    public void setSyncDbName(String syncDbName) {
        this.syncDbName = syncDbName;
    }

    public String getSyncTableName() {
        return syncTableName;
    }

    public void setSyncTableName(String syncTableName) {
        this.syncTableName = syncTableName;
    }

    public String getStoreTableName() {
        return storeTableName;
    }

    public void setStoreTableName(String storeTableName) {
        this.storeTableName = storeTableName;
    }
}
