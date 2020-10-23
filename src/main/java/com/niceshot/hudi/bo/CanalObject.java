package com.niceshot.hudi.bo;

import com.niceshot.hudi.constant.CanalDataType;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author created by chenjun at 2020-10-23 18:14
 */
@Data
public class CanalObject {
    private String table;
    private CanalDataType type;
    private List<Map<String,String>> data;
    private String database;
    private Long es;
    private Long id;
    private Boolean isDdl;
    private Map<String,String> mysqlType;
    private List<Map<String,String>> old;
    private List<String> pkNames;
    private String sql;
    private Map<String,Integer> sqlType;
    private Long ts;
}
