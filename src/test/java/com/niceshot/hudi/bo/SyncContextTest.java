package com.niceshot.hudi.bo;

import com.niceshot.hudi.constant.Constants;
import com.niceshot.hudi.util.CanalDataProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author created by chenjun at 2020-11-16 13:43
 */
public class SyncContextTest {
    private String parameters[] = new String[]
            {"--kafka-server","prod.confluent-common.node1.haixue.local:9092,prod.confluent-common.node2.haixue.local:9092,prod.confluent-common.node3.haixue.local:9092",
                    "--kafka-topic","dt_streaming_canal_highso",
                    "--base-save-path","hdfs://10.100.0.61:8020/hudi_table/",
                    "--duration-seconds","1200",
                    "--sync-table-info-file","/Users/apple/Code/external/hudi-learn/src/test/resources/fetch_canal_table.properties"
            };

    private SyncContext syncContext ;
    CanalObject canalObject;
    @Before
    public void prepare() {
        syncContext = new SyncContext();
        syncContext.init(parameters);
        String canalString = "{\n" +
                "    \"data\": [{\n" +
                "        \"id\": \"5483479\",\n" +
                "        \"chance_id\": \"10786431\",\n" +
                "        \"start_type\": \"18\",\n" +
                "        \"end_type\": null,\n" +
                "        \"start_time\": \"2020-11-05 16:29:30\",\n" +
                "        \"end_time\": null,\n" +
                "        \"assign_count\": \"1\",\n" +
                "        \"location_type\": \"USER\",\n" +
                "        \"location_data_id\": \"116335\",\n" +
                "        \"rule_id\": null,\n" +
                "        \"end_location_type\": null,\n" +
                "        \"end_location_data_id\": null,\n" +
                "        \"allow_share\": \"1\",\n" +
                "        \"chance_type\": \"1\",\n" +
                "        \"advisory_status\": \"1\",\n" +
                "        \"active_type\": \"2\",\n" +
                "        \"assign_way\": \"2\",\n" +
                "        \"ext\": null,\n" +
                "        \"createTime\": \"2020-11-05 16:29:30\",\n" +
                "        \"updated_date\": \"2020-11-05 16:29:30\",\n" +
                "        \"created_by\": \"-1\",\n" +
                "        \"updated_by\": \"-1\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"id\": \"5483480\",\n" +
                "        \"chance_id\": \"10786432\",\n" +
                "        \"start_type\": \"18\",\n" +
                "        \"end_type\": null,\n" +
                "        \"start_time\": \"2020-11-05 16:29:30\",\n" +
                "        \"end_time\": null,\n" +
                "        \"assign_count\": \"1\",\n" +
                "        \"location_type\": \"USER\",\n" +
                "        \"location_data_id\": \"116335\",\n" +
                "        \"rule_id\": null,\n" +
                "        \"end_location_type\": null,\n" +
                "        \"end_location_data_id\": null,\n" +
                "        \"allow_share\": \"1\",\n" +
                "        \"chance_type\": \"1\",\n" +
                "        \"advisory_status\": \"1\",\n" +
                "        \"active_type\": \"2\",\n" +
                "        \"assign_way\": \"2\",\n" +
                "        \"ext\": null,\n" +
                "        \"createTime\": \"2020-11-05 16:30:31\",\n" +
                "        \"updated_date\": \"2020-11-05 16:30:31\",\n" +
                "        \"created_by\": \"-1\",\n" +
                "        \"updated_by\": \"-1\"\n" +
                "    }],\n" +
                "    \"database\": \"crm\",\n" +
                "    \"es\": 1604564995000,\n" +
                "    \"id\": 1,\n" +
                "    \"isDdl\": false,\n" +
                "    \"mysqlType\": null,\n" +
                "    \"old\": null,\n" +
                "    \"pkNames\": null,\n" +
                "    \"sql\": null,\n" +
                "    \"sqlType\": null,\n" +
                "    \"table\": \"orderrecord\",\n" +
                "    \"ts\": 1604564997770,\n" +
                "    \"type\": \"INSERT\"\n" +
                "}";

        canalObject = CanalDataProcessor.parse(canalString);
    }

    @Test
    public void init() {
        //验证配置文件读取
        List<SyncTableInfo> tableInfos = syncContext.getTableInfos();
        SyncTableInfo order = tableInfos.stream().filter(syncTableInfo -> syncTableInfo.getDb().equalsIgnoreCase("order") && syncTableInfo.getTable().equalsIgnoreCase("order")).findFirst().get();
        Assert.assertEquals(order.getPrimaryKey(),"id");
        Assert.assertEquals(order.getPrecombineKey(),"id");
        Assert.assertEquals(order.getPartitionKey(),"create_date");
        Assert.assertEquals("order__order",order.getStoreTable());
        Assert.assertEquals("hdfs://10.100.0.61:8020/hudi_table/order__order",order.getRealSavePath());

        SyncTableInfo orderrecord = tableInfos.stream().filter(syncTableInfo -> syncTableInfo.getDb().equalsIgnoreCase("crm") && syncTableInfo.getTable().equalsIgnoreCase("orderrecord")).findFirst().get();
        Assert.assertEquals(orderrecord.getPrimaryKey(),"id");
        Assert.assertEquals(orderrecord.getPrecombineKey(),"test");
        Assert.assertEquals(orderrecord.getPartitionKey(),"createTime");
        Assert.assertEquals("crm__orderrecord",orderrecord.getStoreTable());
        Assert.assertEquals("hdfs://10.100.0.61:8020/hudi_table/crm__orderrecord",orderrecord.getRealSavePath());
    }

    @Test
    public void isSyncTable() {
        CanalObject canalObject = new CanalObject();
        canalObject.setDatabase("crm");
        canalObject.setTable("orderrecord");
        Assert.assertTrue(syncContext.isSyncTable(canalObject));
    }

    @Test
    public void buildHudiData() {
        HudiHandleObject hudiHandleObject = syncContext.buildHudiData(canalObject);
        hudiHandleObject.getJsonData();
        Assert.assertEquals(Constants.HudiOperationType.UPSERT,hudiHandleObject.getOperationType());
        Assert.assertEquals(2,hudiHandleObject.getJsonData().size());
        String jsonData = hudiHandleObject.getJsonData().get(0);
        Assert.assertTrue(jsonData.contains("createtime"));
        System.out.println(jsonData);
        Assert.assertTrue(jsonData.contains(Constants.HudiTableMeta.PARTITION_KEY));
    }


    @After
    public void clean() {

    }

}