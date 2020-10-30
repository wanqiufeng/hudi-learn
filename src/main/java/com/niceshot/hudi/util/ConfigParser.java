package com.niceshot.hudi.util;

import com.niceshot.hudi.config.HiveMetaSyncConfig;
import com.niceshot.hudi.config.HudiSaveApplicationConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.com.beust.jcommander.JCommander;

/**
 * @author created by chenjun at 2020-10-30 14:59
 */
public class ConfigParser {
    public static HudiSaveApplicationConfig parseHudiDataSaveConfig(String[] args) {
        HudiSaveApplicationConfig config = new HudiSaveApplicationConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        if(StringUtils.isBlank(config.getKafkaGroup())) {
            config.setKafkaGroup(buildKafkaGroup(config));
        } if(StringUtils.isBlank(config.getStoreTableName())) {
            config.setStoreTableName(buildHudiStoreTableName(config));
        } if(StringUtils.isBlank(config.getRealSavePath())) {
            config.setRealSavePath(buildRealSavePath(config));
        }
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

    private static String buildKafkaGroup(HudiSaveApplicationConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("hudi_");
        stringBuilder.append(config.getSyncDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getSyncTableName());
        return stringBuilder.toString();
    }



    private static String buildHudiStoreTableName(HudiSaveApplicationConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getSyncDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getSyncTableName());
        return stringBuilder.toString();
    }

    private static String buildRealSavePath(HudiSaveApplicationConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getBaseSavePath());
        if(!config.getBaseSavePath().endsWith("/")) {
            stringBuilder.append("/");
        }
        stringBuilder.append(config.getStoreTableName());
        return stringBuilder.toString();
    }
}
