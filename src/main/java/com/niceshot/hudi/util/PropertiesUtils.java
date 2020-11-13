package com.niceshot.hudi.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author created by chenjun at 2020-11-13 17:54
 */
public class PropertiesUtils {
    public static Properties loadPropertiesFile(String filePath) {
        Preconditions.checkNotNull(StringUtils.isNoneBlank(filePath), "sync table config can not be null");
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(filePath)) {
            prop.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("load properties error,file path is:" + filePath, ex);
        }
        return prop;
    }

    public static void main(String[] args) {
        Properties properties = PropertiesUtils.loadPropertiesFile("/Users/apple/Code/external/hudi-learn/loca-test-config/fetch_canal_table.properties");
        System.out.println("");
    }
}
