package com.niceshot.hudi.bo;

import com.niceshot.hudi.constant.CanalDataType;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author created by chenjun at 2020-10-23 18:35
 */
@Data
public class HudiHandleObject {
    private String database;
    private String table;
    private CanalDataType type;
    private List<Map<String,Object>> data;
    private Long recordKey;
    private String partitionKey;
}
