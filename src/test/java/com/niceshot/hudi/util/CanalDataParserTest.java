package com.niceshot.hudi.util;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.niceshot.hudi.constant.Constants;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author created by chenjun at 2020-11-11 11:21
 */
public class CanalDataParserTest {

    @org.junit.Test
    public void buildJsonDataString() {
        String partitionKey = "create_date";
        String partitionValue = "";
        List<Map<String,String>> testData = buildTestData(partitionKey,partitionValue);
        List<String> strings = CanalDataParser.buildJsonDataString(testData, partitionKey);
        System.out.println(strings.get(0));
        Assert.assertTrue(strings.get(0).contains(Constants.HudiTableMeta.VOID_DATE_PARTITION_VAL));

        partitionValue = null;
        List<Map<String, String>> testData1 = buildTestData(partitionKey, partitionValue);
        List<String> strings2 = CanalDataParser.buildJsonDataString(testData, partitionKey);
        Assert.assertTrue(strings2.get(0).contains(Constants.HudiTableMeta.VOID_DATE_PARTITION_VAL));
        System.out.println(strings2.get(0));

        partitionValue = "2020-03-04";
        List<Map<String, String>> maps = buildTestData(partitionKey, partitionValue);
        List<String> strings1 = CanalDataParser.buildJsonDataString(maps, partitionKey);
        System.out.println(strings1.get(0));
        Assert.assertTrue(strings1.get(0).contains("2020/03/04"));



    }

    private List<Map<String, String>> buildTestData(String partitionKey, String partitionValue) {
        Map<String, String> objectObjectHashMap = Maps.newHashMap();
        objectObjectHashMap.put("name","niceshot");
        objectObjectHashMap.put(partitionKey,partitionValue);
        List<Map<String, String>> result = Lists.newArrayList();
        result.add(objectObjectHashMap);
        return result;
    }


}