package com.niceshot.hudi.bo;

import com.niceshot.hudi.config.CanalKafkaImport2HudiConfig;
import com.niceshot.hudi.constant.Constants;
import com.niceshot.hudi.util.CanalDataProcessor;
import com.niceshot.hudi.util.ConfigParser;
import com.niceshot.hudi.util.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author created by chenjun at 2020-11-13 14:14
 */
public class SyncContext implements Serializable {
    private CanalKafkaImport2HudiConfig appConfig;
    private List<SyncTableInfo> tableInfos;


    public void init(String[] args) {
        CanalKafkaImport2HudiConfig canalKafkaImport2HudiConfig = ConfigParser.parseHudiCanalDataConfig(args);
        this.appConfig = canalKafkaImport2HudiConfig;
        List<SyncTableInfo> syncTableInfos = this.readSyncTableInfo(appConfig);
        this.tableInfos = syncTableInfos;
    }

    public boolean isSyncTable(SyncTableInfo tableInfo, CanalObject canalObject) {
        return tableInfo.getDb().equalsIgnoreCase(canalObject.getDatabase())
                &&
                tableInfo.getTable().equalsIgnoreCase(canalObject.getTable());
    }

    public boolean isSyncTable(CanalObject canalObject) {
        return tableInfos.stream().anyMatch(tableInfo ->
                isSyncTable(tableInfo,canalObject) );
    }

    public HudiHandleObject buildHudiData(CanalObject canalObject) {
        HudiHandleObject result = CanalDataProcessor.parse(canalObject);
        String partitionKey = findPartitionKey(canalObject);
        List<String> jsonDataString = CanalDataProcessor.buildJsonDataString(result.getData(), partitionKey);
        result.setJsonData(jsonDataString);
        return result;
    }


    public CanalKafkaImport2HudiConfig getAppConfig() {
        return appConfig;
    }

    public void setAppConfig(CanalKafkaImport2HudiConfig appConfig) {
        this.appConfig = appConfig;
    }

    public List<SyncTableInfo> getTableInfos() {
        return tableInfos;
    }

    public void setTableInfos(List<SyncTableInfo> tableInfos) {
        this.tableInfos = tableInfos;
    }

    private String findPartitionKey(CanalObject result) {
        Optional<SyncTableInfo> tableInfo = tableInfos.stream().filter(tableinfo -> isSyncTable(tableinfo,result)).findFirst();
        SyncTableInfo syncTableInfo = tableInfo.get();
        return syncTableInfo.getPartitionKey();
    }

    private List<SyncTableInfo> readSyncTableInfo(CanalKafkaImport2HudiConfig config) {
        Properties properties = PropertiesUtils.loadPropertiesFile(config.getSyncTableInfoFile());
        Set<Pair<String, String>> dbAndTableInfos = properties.keySet().stream().map(key -> obtainDbAndTableInfo(key)).collect(Collectors.toSet());
        return dbAndTableInfos.stream().map(pair -> {
            String db = pair.getFirst();
            String table = pair.getSecond();
            String primaryKey = buildKey(db, table, Constants.SyncTableInfoConfig.PRIMARY_KEY_SUFFIX);
            String primaryKeyValue = properties.getProperty(primaryKey, "id");
            String preCombineKey = buildKey(db, table, Constants.SyncTableInfoConfig.PRECOMBINE_KEY_SUFFIX);
            String preCombineKeyValue = properties.getProperty(preCombineKey, "id");
            String partitionKey = buildKey(db, table, Constants.SyncTableInfoConfig.PARTITION_KEY_SUFFIX);
            String partitionKeyValue = properties.getProperty(partitionKey);
            SyncTableInfo syncTableInfo = new SyncTableInfo();
            syncTableInfo.setDb(db);
            syncTableInfo.setTable(table);
            syncTableInfo.setPrimaryKey(primaryKeyValue);
            syncTableInfo.setPrecombineKey(preCombineKeyValue);
            syncTableInfo.setPartitionKey(partitionKeyValue);
            String storeTableName = ConfigParser.buildHudiStoreTableName(db, table);
            syncTableInfo.setStoreTable(storeTableName);
            syncTableInfo.setRealSavePath(ConfigParser.buildRealSavePath(config.getBaseSavePath(),storeTableName));
            return syncTableInfo;
        }).collect(Collectors.toList());
    }


    private String buildRealSavePath(CanalKafkaImport2HudiConfig config, String storeTable) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getBaseSavePath());
        if (!config.getBaseSavePath().endsWith("/")) {
            stringBuilder.append("/");
        }
        stringBuilder.append(storeTable);
        return stringBuilder.toString();
    }

    private String buildKey(String db, String table, String primaryKeySuffix) {
        return StringUtils.joinWith(".", db, table, primaryKeySuffix);
    }

    private Pair<String, String> obtainDbAndTableInfo(Object key) {
        String keyStr = String.valueOf(key);
        String[] split = keyStr.split("\\.");
        return Pair.create(split[0], split[1]);
    }
}
