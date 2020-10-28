package com.niceshot.hudi.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.niceshot.hudi.bo.CanalObject;
import com.niceshot.hudi.bo.HudiHandleObject;
import com.niceshot.hudi.constant.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author created by chenjun at 2020-10-23 18:10
 */
public class CanalDataParser {
    private static Set<String> allowedOparation = Sets.newHashSet(Constants.CanalOperationType.INSERT, Constants.CanalOperationType.UPDATE, Constants.CanalOperationType.DELETE);
    private static Map<String, String> canalOperationMapping2HudiOperation = Maps.newHashMap();

    static {
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.INSERT,Constants.HudiOperationType.UPSERT );
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.UPDATE,Constants.HudiOperationType.UPSERT );
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.DELETE,Constants.HudiOperationType.DELETE );
    }

    /**
     * 将canal原始数据转化成Hudi可以使用的数据信息
     *
     * @param originalCanalData
     * @return
     * @throws IOException
     */
    public static HudiHandleObject parse(String originalCanalData,String partitionDateField) throws IOException {
        Preconditions.checkNotNull(originalCanalData, "canal data can not be null");
        //直接一次性解析成对象, 从对象中提取出想要的东西
        //如果不为自己关注的操作类型，直接范围
        //为自己操作的类型，构建对应对象
        CanalObject canalObject = JsonUtils.getObjectMapper().readValue(originalCanalData, CanalObject.class);
        Preconditions.checkNotNull(canalObject, "canal object can not be null");
        Preconditions.checkNotNull(canalObject.getTable(), "canal op type  can not be null ");
        if (!allowedOparation.contains(canalObject.getType())) {
            return null;
        }
        HudiHandleObject result = new HudiHandleObject();
        result.setOperationType(canalOperationMapping2HudiOperation.get(canalObject.getType()));
        result.setDatabase(canalObject.getDatabase());
        result.setTable(canalObject.getTable());
        result.setData(buildTypeData(canalObject,partitionDateField));
        return result;
    }



    /**
     * 将canal中的map数据，转化成json List
     *
     * @param canalObject
     * @param partitionDateField
     * @return
     */
    private static List<String> buildTypeData(CanalObject canalObject, String partitionDateField) {
        //找出其中的数据
        //从type中，找到其对应的数据类型
        //将这其value转化成对应对象
        List<Map<String, String>> data = canalObject.getData();
        Map<String, Integer> mysqlType = canalObject.getSqlType();
        return data.stream().map(dataMap -> addHudiRecognizePartition(dataMap,partitionDateField,mysqlType))
                .map(stringObjectMap -> JsonUtils.toJson(stringObjectMap))
                .collect(Collectors.toList());
    }

    private static Map<String,String> addHudiRecognizePartition(Map<String, String> dataMap, String partitionDateField, Map<String, Integer> mysqlType) {
        String partitionOriginalValue = dataMap.get(partitionDateField);
        Integer sqlType = mysqlType.get(partitionDateField);
        Preconditions.checkArgument(StringUtils.isNotBlank(partitionOriginalValue),"partition value can not be null");
        String hudiPartitionFormatValue;
        if(Types.TIMESTAMP == sqlType) {
            hudiPartitionFormatValue = DateUtils.dateStringFormat(partitionOriginalValue, DateUtils.DATE_FORMAT_YYYY_MM_DD_hh_mm_ss, DateUtils.DATE_FORMAT_YYYY_MM_DD_SLASH);
        } else if(Types.DATE == sqlType) {
            hudiPartitionFormatValue = DateUtils.dateStringFormat(partitionOriginalValue,DateUtils.DATE_FORMAT_YYYY_MM_DD,DateUtils.DATE_FORMAT_YYYY_MM_DD_SLASH);
        } else {
            throw new RuntimeException("partition field must be any type of [datetime,timestamp,date] ,current sqlType is :"+sqlType);
        }
        dataMap.put(Constants.HudiTableMeta.PARTITION_KEY,hudiPartitionFormatValue);
        return dataMap;
    }

    /**
     * canal发送的data中，所有类型的值都是以字符串体现。该方法用于将字符串值，转化成对应的实际类型
     *
     * @param stringValueMap
     * @param mysqlType
     * @param partitionDateField
     * @return
     */
    public static void main(String[] args) throws IOException {
        //将原始数据读取成一个ObjectNode
        //读取其中的data节点、type节点(insert 、update、delete的binlog)、mysqlType
        //构建HudiHandleObject
        String demoString = "{\n" +
                "    \"data\": [{\n" +
                "        \"id\": \"4\",\n" +
                "        \"name\": \"test\",\n" +
                "        \"new_col\": null\n" +
                "    }],\n" +
                "    \"database\": \"test\",\n" +
                "    \"es\": 1603446001000,\n" +
                "    \"id\": 200360,\n" +
                "    \"isDdl\": false,\n" +
                "    \"mysqlType\": {\n" +
                "        \"id\": \"bigint(20)\",\n" +
                "        \"name\": \"varchar(50)\",\n" +
                "        \"new_col\": \"varchar(100)\"\n" +
                "    },\n" +
                "    \"old\": null,\n" +
                "    \"pkNames\": [\"id\"],\n" +
                "    \"sql\": \"\",\n" +
                "    \"sqlType\": {\n" +
                "        \"id\": -5,\n" +
                "        \"name\": 12,\n" +
                "        \"new_col\": 12\n" +
                "    },\n" +
                "    \"table\": \"test_binglog\",\n" +
                "    \"ts\": 1603446001498,\n" +
                "    \"type\": \"INSERT\"\n" +
                "}";
        // json操作相关资料：https://stackoverflow.com/questions/26190851/get-single-field-from-json-using-jackson
        //HudiHandleObject parse = CanalDataParser.parse(demoString);
        System.out.println("test");
    }
}
