package com.niceshot.hudi

import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.niceshot.hudi.bo.{HudiHandleObject, SyncContext, SyncTableInfo}
import com.niceshot.hudi.constant.Constants
import com.niceshot.hudi.util.CanalDataProcessor
import com.typesafe.scalalogging.Logger
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, _}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
    val syncContext = new SyncContext()
    syncContext.init(args)
    //todo: check config
    val config = syncContext.getAppConfig
    val appName = "hudi_sync_canal_app"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(config.getDurationSeconds))
    val spark = SparkSession.builder().config(conf).getOrCreate();

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getKafkaServer,
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> config.getKafkaGroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false",
      "session.timeout.ms" -> "30000"
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
        val dataRdd = recordRDD.map(consumerRecord => CanalDataProcessor.parse(consumerRecord.value()))
          .filter(record => syncContext.isSyncTable(record))
          .map(record => syncContext.buildHudiData(record))
        val computeArray: Array[CompletableFuture[Void]] = Array()
        for (syncTable <- syncContext.getTableInfos) {
          val computeUnit = CompletableFuture.runAsync(new Runnable {
            override def run(): Unit = {
              try {
                val syncTableRdd = dataRdd.filter(record => syncTable.getDb.equalsIgnoreCase(record.getDatabase) && syncTable.getTable.equalsIgnoreCase(record.getTable))
                tableDataOperation(spark, syncTable, syncTableRdd, logger)
              } catch {
                case exception: Exception => logger.error(exception.getMessage, exception)
              }
            }
          })
          computeArray :+ computeUnit
        }
        CompletableFuture.allOf(computeArray: _*).get(3,TimeUnit.HOURS)
      } catch {
        case exception: Exception => logger.error(exception.getMessage, exception)
      } finally {
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }


  private def tableDataOperation(spark: SparkSession, syncTable: SyncTableInfo, syncTableRdd: RDD[HudiHandleObject], logger: Logger): Unit = {
    logger.info("结果集不为空，开始操作")
    val upsertDf = syncTableRdd.filter(record => record.getOperationType != Constants.HudiOperationType.DELETE)
    val deleteDf = syncTableRdd.filter(record => record.getOperationType == Constants.HudiOperationType.DELETE)
    if (!upsertDf.isEmpty()) {
      logger.info("开始更新操作")
      val upsertData = upsertDf.flatMap(data => data.getJsonData)
      val df = spark.read.json(upsertData)
      hudiDataUpsertOrDelete(syncTable, df, UPSERT_OPERATION_OPT_VAL)
    }
    //基于自增id的crud，insert和update一定是在delete之前。因为一旦delete后，你再insert, 一定是新的id，不会存在反复insert同一个id的情况
    //所以可以把delete操作，统一放到最后操作
    if (!deleteDf.isEmpty()) {
      logger.info("开始删除操作")
      val deleteData = deleteDf.map(hudiData => {
        hudiData.getJsonData
      }).flatMap(data => data)
      val df = spark.read.json(deleteData)
      hudiDataUpsertOrDelete(syncTable, df, DELETE_OPERATION_OPT_VAL)
    }
  }

  private def hudiDataUpsertOrDelete(syncTable: SyncTableInfo, data: DataFrame, optType: String): Unit = {
    data.write.format("hudi").
      option(OPERATION_OPT_KEY, optType).
      option(PRECOMBINE_FIELD_OPT_KEY, syncTable.getPrecombineKey).
      option(RECORDKEY_FIELD_OPT_KEY, syncTable.getPrimaryKey).
      option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
      option(TABLE_NAME, syncTable.getStoreTable).
      option(TABLE_TYPE_OPT_KEY, MOR_TABLE_TYPE_OPT_VAL).
      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, true).
      option(INSERT_DROP_DUPS_OPT_KEY, true).
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
      save(syncTable.getRealSavePath)
  }
}
