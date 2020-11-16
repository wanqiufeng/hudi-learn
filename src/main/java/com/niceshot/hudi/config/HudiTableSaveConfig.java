package com.niceshot.hudi.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * @author created by chenjun at 2020-11-02 14:33
 */
public class HudiTableSaveConfig implements Serializable {
    //to-do:mark 添加非空约束
    @Parameter(names = {"--base-save-path"},description = "base data file path for hudi table store, has no default value.eg:hdfs://192.168.16.181:8020/hudi_data/")
    private String baseSavePath;
    public String getBaseSavePath() {
        return baseSavePath;
    }
    public void setBaseSavePath(String baseSavePath) {
        this.baseSavePath = baseSavePath;
    }


}
