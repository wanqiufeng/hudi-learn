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
}
