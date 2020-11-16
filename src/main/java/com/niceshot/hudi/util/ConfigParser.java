package com.niceshot.hudi.util;

import com.niceshot.hudi.config.HiveImport2HudiConfig;
import com.niceshot.hudi.config.HiveMetaSyncConfig;
import com.niceshot.hudi.config.CanalKafkaImport2HudiConfig;
import com.niceshot.hudi.config.HudiTableSaveConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.com.beust.jcommander.JCommander;

/**
 * @author created by chenjun at 2020-10-30 14:59
 */
public class ConfigParser {
    public static CanalKafkaImport2HudiConfig parseHudiCanalDataConfig(String[] args) {
        CanalKafkaImport2HudiConfig config = new CanalKafkaImport2HudiConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        return config;
    }

    public static HiveMetaSyncConfig parseHiveMetaSyncConfig(String[] args) {
        HiveMetaSyncConfig config = new HiveMetaSyncConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        return config;
    }

    public static HiveImport2HudiConfig parseHiveImport2HudiConfig(String[] args) {
        HiveImport2HudiConfig config = new HiveImport2HudiConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        setDefaultValueForHudiSave(config);
        return config;
    }


    private static void setDefaultValueForHudiSave(HiveImport2HudiConfig config) {
        if(StringUtils.isBlank(config.getStoreTableName())) {
            config.setStoreTableName(buildHudiStoreTableName(config.getMappingMysqlDbName(),config.getMappingMysqlTableName()));
        }
        if(StringUtils.isBlank(config.getRealSavePath())) {
            config.setRealSavePath(buildRealSavePath(config.getBaseSavePath(),config.getStoreTableName()));
        }
    }

    public static String buildHudiStoreTableName(String dbName,String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(dbName);
        stringBuilder.append("__");
        stringBuilder.append(tableName);
        return stringBuilder.toString();
    }

    public static String buildRealSavePath(String hudiBasePath,String hudiStoreName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(hudiBasePath);
        if(!hudiBasePath.endsWith("/")) {
            stringBuilder.append("/");
        }
        stringBuilder.append(hudiStoreName);
        return stringBuilder.toString();
    }
}
