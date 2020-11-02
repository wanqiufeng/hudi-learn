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
    public static CanalKafkaImport2HudiConfig parseHudiDataSaveConfig(String[] args) {
        CanalKafkaImport2HudiConfig config = new CanalKafkaImport2HudiConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        if(StringUtils.isBlank(config.getKafkaGroup())) {
            config.setKafkaGroup(buildKafkaGroup(config));
        }
        setDefaultValueForHudiSave(config);
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


    private static void setDefaultValueForHudiSave(HudiTableSaveConfig config) {
        if(StringUtils.isBlank(config.getStoreTableName())) {
            config.setStoreTableName(buildHudiStoreTableName(config));
        }
        if(StringUtils.isBlank(config.getRealSavePath())) {
            config.setRealSavePath(buildRealSavePath(config));
        }
    }

    private static String buildKafkaGroup(CanalKafkaImport2HudiConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("hudi_");
        stringBuilder.append(config.getMappingMysqlDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getMappingMysqlTableName());
        return stringBuilder.toString();
    }



    private static String buildHudiStoreTableName(HudiTableSaveConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getMappingMysqlDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getMappingMysqlTableName());
        return stringBuilder.toString();
    }

    private static String buildRealSavePath(HudiTableSaveConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getBaseSavePath());
        if(!config.getBaseSavePath().endsWith("/")) {
            stringBuilder.append("/");
        }
        stringBuilder.append(config.getStoreTableName());
        return stringBuilder.toString();
    }
}
