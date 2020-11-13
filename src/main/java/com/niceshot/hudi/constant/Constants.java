package com.niceshot.hudi.constant;

/**
 * @author created by chenjun at 2020-10-26 14:54
 */
public interface Constants {
    interface CanalOperationType {
        String INSERT = "INSERT";
        String UPDATE = "UPDATE";
        String DELETE = "DELETE";
    }

    interface HudiOperationType {
        String UPSERT = "upsert";
        String INSERT = "insert";
        String DELETE = "delete";
    }

    interface HudiTableMeta {
        String PARTITION_KEY = "_partition_key";
        String VOID_DATE_PARTITION_VAL = "9999/12/30";
    }

    interface SyncTableInfoConfig {
        String PARTITION_KEY_SUFFIX = "partitionkey";
        String PRIMARY_KEY_SUFFIX = "primarykey";
        String PRECOMBINE_KEY_SUFFIX = "precombinekey";
    }
}
