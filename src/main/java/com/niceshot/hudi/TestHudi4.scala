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
object TestHudi4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate();
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load("hdfs://192.168.16.181:8020/hudi_data/hudi_hive_test32" + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    println("first query===========>")
    spark.sql("select * from  hudi_trips_snapshot ").show()
  }
}
