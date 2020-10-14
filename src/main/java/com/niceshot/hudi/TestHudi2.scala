package com.niceshot.hudi

import org.apache.http.client.methods.HttpRequestBase
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
 * @author created by chenjun at 2020-10-14 15:32
 *
 */
object TestHudi2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[2]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val tableName = "hudi_trips_cow"
    //val basePath = "file:/Users/apple/Temp/hudi_data"
    val basePath = "hdfs://master:8020/hudi_test"
    val dataGen = new DataGenerator
    //val inserts = convertToStringList(dataGen.generateInserts(10))
    val inserts = List("{\"ts\": 0.0, \"uuid\": \"7fa0bf0a-ed9a-4bf0-bd63-7b163d39781a\", \"rider\": \"rider-214\", \"driver\": \"driver-213\", \"begin_lat\": 0.4726905879569653, \"begin_lon\": 0.46157858450465483, \"end_lat\": 0.754803407008858, \"end_lon\": 0.9671159942018241, \"fare\": 34.158284716382845, \"partitionpath\": \"americas/brazil/sao_paulo\"}")
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      //option(OPERATION_OPT_KEY,"delete").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(SaveMode.Append).
      save(basePath)



    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    println("first query===========>")
    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    println("second query===========>")
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

  }
}
