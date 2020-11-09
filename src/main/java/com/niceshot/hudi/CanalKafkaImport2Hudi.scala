package com.niceshot.hudi

import com.niceshot.hudi.config.CanalKafkaImport2HudiConfig
import com.niceshot.hudi.constant.Constants
import com.niceshot.hudi.util.{CanalDataParser, ConfigParser}
import com.typesafe.scalalogging.Logger
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, _}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
 * @author created by chenjun at 2020-10-14 15:32
 *
 */
object CanalKafkaImport2Hudi {
  def main(args: Array[String]): Unit = {
    val logger = Logger("com.niceshot.hudi.CanalKafkaImport2Hudi")
    val config = ConfigParser.parseHudiDataSaveConfig(args)
    val appName = "hudi_sync_" + config.getMappingMysqlDbName + "__" + config.getMappingMysqlTableName
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(config.getDurationSeconds))
    val spark = SparkSession.builder().config(conf).getOrCreate();

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getKafkaServer,
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> config.getKafkaGroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )
    val topics = Array(config.getKafkaTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(recordRDD => {
      val offsetRanges = recordRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      try {
        val needOperationData = recordRDD.map(consumerRecord => CanalDataParser.parse(consumerRecord.value(), config.getPartitionKey))
          .filter(consumerRecord => consumerRecord != null && consumerRecord.getDatabase == config.getMappingMysqlDbName && consumerRecord.getTable == config.getMappingMysqlTableName)
        if (needOperationData.isEmpty()) {
          logger.info("空结果集，不做操作")
        } else {
          logger.info("结果集不为空，开始操作")

          val hasNotDelete = needOperationData.filter(record => record.getOperationType == Constants.HudiOperationType.DELETE).isEmpty()
          if (hasNotDelete) {
            //如果没有删除操作，则走批量upsert
            val upsertData = needOperationData.map(hudiData => {
              CanalDataParser.buildJsonDataString(hudiData.getData, config.getPartitionKey)
            }).flatMap(data => data)
            val df = spark.read.json(upsertData)
            hudiDataUpsertOrDelete(config, df, UPSERT_OPERATION_OPT_VAL)
          } else {
            //如果有删除操作，那么必须把数据拉回本地，进行操作
            val localData = needOperationData.collect()
            localData.foreach(record => {
              val jsonDataList = CanalDataParser.buildJsonDataString(record.getData, config.getPartitionKey)
              val df = spark.read.json(spark.sparkContext.parallelize(jsonDataList, 5))
              hudiDataUpsertOrDelete(config, df, record.getOperationType)
            })
          }
        }
      } catch {
        case exception: Exception => logger.error(exception.getMessage,exception)
      } finally {
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }

  private def hudiDataUpsertOrDelete(config: CanalKafkaImport2HudiConfig, data: DataFrame, optType: String): Unit = {
    data.write.format("hudi").
      option(OPERATION_OPT_KEY, optType).
      option(PRECOMBINE_FIELD_OPT_KEY, config.getPrecombineKey).
      option(TABLE_TYPE_OPT_KEY,MOR_TABLE_TYPE_OPT_VAL).
      option(RECORDKEY_FIELD_OPT_KEY, config.getPrimaryKey).
      option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
      option(TABLE_NAME, config.getStoreTableName).
      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, true).
      /*
        关闭hive数据结构实时同步，新增数据量大时，每次都去连hive metastore，怕扛不住
        option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY,hudiTableName).
        option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, hudiTableName).
        option(DataSourceWriteOptions.META_SYNC_ENABLED_OPT_KEY, true).
        option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
        option(DataSourceWriteOptions.HIVE_USER_OPT_KEY, "hive").
        option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY, "hive").
        option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.16.181:10000").
        option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY,Constants.HudiTableMeta.PARTITION_KEY).
        */
      mode(SaveMode.Append).
      save(config.getRealSavePath)
  }
}
