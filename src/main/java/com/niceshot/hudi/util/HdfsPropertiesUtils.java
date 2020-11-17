package com.niceshot.hudi.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author created by chenjun at 2020-11-13 17:54
 */
public class HdfsPropertiesUtils {
    public static Properties loadHdfsPropertiesFile(String propertiesPathInHdfs) {
        Preconditions.checkNotNull(StringUtils.isNoneBlank(propertiesPathInHdfs), "sync table config can not be null");
        Properties prop = new Properties();
        try {
            Configuration conf = new Configuration();
            //conf.addResource(new Path("/Users/apple/Code/external/hudi-learn/loca-test-config/hive-site.xml"));
            FSDataInputStream in = null;
            // Hadoop DFS Path - Input file
            Path inFile = new Path(propertiesPathInHdfs);
            FileSystem fs = inFile.getFileSystem(conf);
            // Check if input is valid
            if (!fs.exists(inFile)) {
                throw new IOException("properties file not found,input file path is:"+propertiesPathInHdfs);
            }
            try {
                // open and read from file
                in = fs.open(inFile);
                prop.load(in);
            }finally {
                IOUtils.closeStream(in);
            }
        } catch (IOException e) {
            throw new RuntimeException("read properties file in hdfs file error");
        }
        return prop;
    }

    public static void main(String[] args) {
        Properties properties = loadHdfsPropertiesFile("hdfs://192.168.16.181:8020/hudi_config/config.properties");
        System.out.println(properties.get("order.order.partitionkey"));
    }
}
