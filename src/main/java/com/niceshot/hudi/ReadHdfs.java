package com.niceshot.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author created by chenjun at 2020-11-17 11:27
 */
public class ReadHdfs {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/apple/Code/external/hudi-learn/loca-test-config/hive-site.xml"));
        //conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        /*System.out.println("Enter the file path...");
        String filePath = br.readLine();*/

        Path path = new Path("hdfs://192.168.16.181:8020/hudi_config/config.properties");
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        IOUtils.copyBytes(inputStream, System.out, 512, false);
        System.out.println(inputStream.available());
        fs.close();
    }
}
