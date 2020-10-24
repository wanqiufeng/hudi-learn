package com.niceshot.hudi.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author created by chenjun at 2020-10-23 18:10
 */
public class CanalDataParser {
    // https://gist.github.com/downgoon/150aa7e9a5826400a1311ca876fc2aee
    public static String parse(String originalCanalData) {
        //直接将获取到的数据，解析为一个json对象。然后去修改它
        return "";
    }

    public static void main(String[] args) throws IOException {
        //
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
        ObjectNode jsonNodes = JsonUtils.getObjectMapper().readValue(demoString, ObjectNode.class);
        JsonNode data = jsonNodes.get("data");
        List<Map<String, String>> result = JsonUtils.getObjectMapper().readValue(data.toString(), new TypeReference<List<Map<String, String>>>() {
        });
        System.out.println(data);
    }


}
