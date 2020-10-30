package com.niceshot.hudi

import org.apache.commons.lang3.ObjectUtils.mode
import org.apache.http.client.methods.HttpRequestBase
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * @author created by chenjun at 2020-10-14 15:32
 *
 */
object TestHudi3 {
  def main(args: Array[String]): Unit = {

    //第一次数据怎么批量导入
    //用spark直接拉取对应表在hive中昨天的分区，将其导入到hudi
    //先将对应的接到canal
    //再将对应的表，通过spark或其它方式，全量拉取到hudi表
    //由于canal先接。全量数据后拉。所以会出现以下几种情况
    //拉取的数据是已更新后的，canal的消费再更新一次。没有问题，因为hudi是基于主键来的，只要主键没变，数据再更新一次也无妨
    //拉取数据是已经删除后的数据，cannal再删一次，已经没有这个主键，存在，大不了删除不成功，数据还是一致的


    //表结构变更怎么响应？
    //hudi会自动扩展字段。hive这边可以将对应的表删掉，下一次数据会自动新建新的表结构

    //binlog消费？
    //接收canal数据，将其转化为一个完成json，并对应其操作类型，比如删除，upsert
    //将该json包装成一个df并执行


    //spark程序如何解决日志问题


    //程序需要提供哪些参数？
    //表名
    //路径名
    //应用名，应用面可以根据表名自动拼

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate();

    //System.setProperty("hadoop.home.dir", "C:\\Users\\wanqi\\DevTools\\hadoop-dev")
    //加上述代码的原因：https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha

    val tableName = "hudi_hive_test23"
    //val basePath = "file:/Users/apple/Temp/hudi_data"
    val basePath = "hdfs://192.168.16.181:8020/hudi_hive_test23"
    val dataGen = new DataGenerator
    //val inserts = convertToStringList(dataGen.generateInserts(10))
    val inserts = List("{\n    \"volume\": 456506,\n    \"symbol\": \"FB\",\n    \"ts\": \"2018-08-31 09:30:00\",\n    \"month\": \"08\",\n    \"high\": 177.5,\n    \"low\": 176.465,\n    \"key\": \"FB_2018-08-31 09\",\n    \"year\": 2018,\n    \"date\": \"2018/08/31\",\n    \"close\": 176.83,\n    \"open\": 177.29,\n    \"day\": \"31\",\n \"add_clo\": \"haha\"}")
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      //option(OPERATION_OPT_KEY,"delete").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "key").
      option(PARTITIONPATH_FIELD_OPT_KEY, "date").
      //option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,"").
      option(TABLE_NAME, "hudi_hive_test24").
      option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY,tableName).
      option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, tableName).
      //option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, true).
      option(DataSourceWriteOptions.META_SYNC_ENABLED_OPT_KEY, true).
      option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
      option(DataSourceWriteOptions.HIVE_USER_OPT_KEY, "hive").
      option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY, "hive").
      option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.16.181:10000").
      option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "date").
      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY,true).
      mode(SaveMode.Append).
      save(basePath)


    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    println("first query===========>")
    spark.sql("select * from  hudi_trips_snapshot ").show()
  }
}
