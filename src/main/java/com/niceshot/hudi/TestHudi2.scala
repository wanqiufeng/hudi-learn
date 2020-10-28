package com.niceshot.hudi

import com.niceshot.hudi.constant.Constants
import com.niceshot.hudi.util.CanalDataParser
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, _}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
 * @author created by chenjun at 2020-10-14 15:32
 *
 */
object TestHudi2 {
  def main(args: Array[String]): Unit = {
    val appName = "Simple Application"
    val kafkaServer = "192.168.16.237:9092,192.168.16.236:9092"
    val kafkaConsumerGroupId = "use_a_separate_group_id_for_each_stream"
    val kafkaConsumerOffset = "latest"
    val kafkaEnableAutoCommit = "false"
    val kafkaConsumeTopic="test"
    val hudiTableName="hudi_hive_test20"
    val baseHudiTablePath = "hdfs://192.168.16.181:8020/hudi_data/"
    val realHudiTablePath = baseHudiTablePath + hudiTableName
    val precombineField = "id"
    val recordKey = "id"
    val partitionField="create_date"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(2))
    val spark = SparkSession.builder().config(conf).getOrCreate();

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> kafkaConsumerGroupId,
      "auto.offset.reset" -> kafkaConsumerOffset,
      "enable.auto.commit" ->kafkaEnableAutoCommit
    )

    val topics = Array(kafkaConsumeTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(record=>{
      val hudiDataPackage = CanalDataParser.parse(record.value(),partitionField)
      hudiDataPackage
    }).filter(record=> {
      record != null
    }).foreachRDD(recordRDD=> {
      try {
        val hasNotDelete = recordRDD.filter(hudiData=> {
          hudiData.getOperationType == Constants.HudiOperationType.DELETE
        }).isEmpty()
        print("是否没有删除操作:"+hasNotDelete)
        if(hasNotDelete) {
          //如果没有删除操作，则走批量upsert
          val upsertData = recordRDD.map(hudiData=>{hudiData.getData}).flatMap(data=>data)
          val df = spark.read.json(upsertData)
          df.write.format("hudi").
            option(OPERATION_OPT_KEY,UPSERT_OPERATION_OPT_VAL).
            options(getQuickstartWriteConfigs).
            option(PRECOMBINE_FIELD_OPT_KEY,precombineField).
            option(RECORDKEY_FIELD_OPT_KEY, recordKey).
            option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
            option(TABLE_NAME, hudiTableName).
            option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY,hudiTableName).
            option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, hudiTableName).
            option(DataSourceWriteOptions.META_SYNC_ENABLED_OPT_KEY, true).
            option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
            option(DataSourceWriteOptions.HIVE_USER_OPT_KEY, "hive").
            option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY, "hive").
            option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.16.181:10000").
            option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY,Constants.HudiTableMeta.PARTITION_KEY).
            option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY,true).
            mode(SaveMode.Append).
            save(realHudiTablePath)
        } else {
          //如果有删除操作，那么必须把数据拉回本地，进行操作
          val localData = recordRDD.collect()
          localData.foreach(record=> {
            val df = spark.read.json(spark.sparkContext.parallelize(record.getData, 2))
            df.write.format("hudi").
              option(OPERATION_OPT_KEY,record.getOperationType).
              options(getQuickstartWriteConfigs).
              option(PRECOMBINE_FIELD_OPT_KEY,precombineField).
              option(RECORDKEY_FIELD_OPT_KEY, recordKey).
              option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
              option(TABLE_NAME, hudiTableName).
              option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY,hudiTableName).
              option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, hudiTableName).
              option(DataSourceWriteOptions.META_SYNC_ENABLED_OPT_KEY, true).
              option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
              option(DataSourceWriteOptions.HIVE_USER_OPT_KEY, "hive").
              option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY, "hive").
              option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.16.181:10000").
              option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY,Constants.HudiTableMeta.PARTITION_KEY).
              option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY,true).
              mode(SaveMode.Append).
              save(realHudiTablePath)
          })
        }
      } catch {
        case exception: Exception => print(exception)
      }

    })
    ssc.start()
    ssc.awaitTermination()
    //System.setProperty("hadoop.home.dir", "C:\\Users\\wanqi\\DevTools\\hadoop-dev")
    //加上述代码的原因：https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
  }
}
