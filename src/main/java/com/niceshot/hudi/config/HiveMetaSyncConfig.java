package com.niceshot.hudi.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * @author created by chenjun at 2020-10-30 14:22
 */
public class HiveMetaSyncConfig implements Serializable {
    @Parameter(names = {"--hive-db-name"},description = "the hive db will sync to ")
    private String hiveDbName;
    @Parameter(names = {"--hive-table-name"},description = "the hive table will create")
    private String hiveTableName;
    @Parameter(names = {"--hive-user-name"},description = "default is 'hive'")
    private String hiveUser;
    @Parameter(names = {"--hive-pwd"},description = "default is 'hive'")
    private String hivePwd;
    @Parameter(names = {"--hive-jdbc-url"},description = "eg:jdbc:hive2://192.168.16.181:10000")
    private String hiveJdbcUrl;
    @Parameter(names = {"--hudi-table-path"},description = "the file system path which hudi table data stored ,eg:hdfs://192.168.16.181:8020/hudi_data/hudi_hive_test33")
    private String hudiTablePath;
    @Parameter(names = {"--hive-site-path"},description = "hive-site.xml path")
    private String hiveConfFilePath;

    public String getHiveDbName() {
        return hiveDbName;
    }

    public void setHiveDbName(String hiveDbName) {
        this.hiveDbName = hiveDbName;
    }

    public String getHiveUser() {
        return hiveUser;
    }

    public void setHiveUser(String hiveUser) {
        this.hiveUser = hiveUser;
    }

    public String getHivePwd() {
        return hivePwd;
    }

    public void setHivePwd(String hivePwd) {
        this.hivePwd = hivePwd;
    }

    public String getHiveJdbcUrl() {
        return hiveJdbcUrl;
    }

    public void setHiveJdbcUrl(String hiveJdbcUrl) {
        this.hiveJdbcUrl = hiveJdbcUrl;
    }


    public String getHiveTableName() {
        return hiveTableName;
    }

    public void setHiveTableName(String hiveTableName) {
        this.hiveTableName = hiveTableName;
    }

    public String getHudiTablePath() {
        return hudiTablePath;
    }

    public void setHudiTablePath(String hudiTablePath) {
        this.hudiTablePath = hudiTablePath;
    }

    public String getHiveConfFilePath() {
        return hiveConfFilePath;
    }

    public void setHiveConfFilePath(String hiveConfFilePath) {
        this.hiveConfFilePath = hiveConfFilePath;
    }
}
