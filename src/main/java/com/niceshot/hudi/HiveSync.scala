package com.niceshot.hudi

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{HIVE_BASE_FILE_FORMAT_OPT_KEY, HIVE_DATABASE_OPT_KEY, HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, HIVE_PARTITION_FIELDS_OPT_KEY, HIVE_PASS_OPT_KEY, HIVE_TABLE_OPT_KEY, HIVE_URL_OPT_KEY, HIVE_USER_OPT_KEY, HIVE_USE_JDBC_OPT_KEY, HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer

/**
 * @author created by chenjun at 2020-10-29 15:42
 *
 */
object HiveSync {
  def main(args: Array[String]): Unit = {
    //构造sparksession对象
    val spark = SparkSession
      .builder
      .appName("delta hiveSync")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()

    val basePath = new Path("hdfs://192.168.16.181:8020/hudi_data/hudi_hive_test33")
    val parameters = Map(
      "hoodie.datasource.write.insert.drop.duplicates"->"false",
      "hoodie.datasource.hive_sync.database"->"default",
      "hoodie.datasource.write.row.writer.enable"->"false",
      "hoodie.insert.shuffle.parallelism"->"2",
      "path"->"hdfs://192.168.16.181:8020/hudi_data/hudi_hive_test33",
      "hoodie.datasource.write.precombine.field"->"id",
      "hoodie.datasource.hive_sync.partition_fields"->"_partition_date",
      "hoodie.datasource.write.payload.class"->"org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "hoodie.datasource.hive_sync.use_jdbc"->"true",
      "hoodie.datasource.hive_sync.partition_extractor_class"->"org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
      "hoodie.datasource.meta.sync.enable"->"true",
      "hoodie.datasource.write.streaming.retry.interval.ms"->"2000",
      "hoodie.datasource.hive_sync.base_file_format"->"PARQUET",
      "hoodie.datasource.hive_sync.table"->"hudi_hive_test33",
      "hoodie.index.type"->"GLOBAL_BLOOM",
      "hoodie.datasource.write.streaming.ignore.failed.batch"->"true",
      "hoodie.datasource.write.operation"->"upsert",
      "hoodie.datasource.hive_sync.enable"->"true",
      "hoodie.datasource.write.recordkey.field"->"id",
      "hoodie.table.name"->"hudi_hive_test33",
      "hoodie.datasource.hive_sync.jdbcurl"->"jdbc:hive2://192.168.16.181:10000",
      "hoodie.datasource.write.table.type"->"COPY_ON_WRITE",
      "hoodie.datasource.write.hive_style_partitioning"->"true",
      "hoodie.bloom.index.update.partition.path"->"true",
      "hoodie.datasource.hive_sync.username"->"hive",
      "hoodie.datasource.write.streaming.retry.count"->"3",
      "hoodie.datasource.compaction.async.enable"->"true",
      "hoodie.datasource.hive_sync.password"->"hive",
      "hoodie.datasource.write.keygenerator.class"->"org.apache.hudi.keygen.SimpleKeyGenerator",
      "hoodie.upsert.shuffle.parallelism"->"2",
      "hoodie.meta.sync.client.tool.class"->"org.apache.hudi.hive.HiveSyncTool",
      "hoodie.datasource.write.partitionpath.field"->"_partition_date",
      "hoodie.datasource.write.commitmeta.key.prefix"->"_"
    )
    val hiveSyncConfig: HiveSyncConfig = buildSyncConfig(basePath, parameters)
    val hiveConf: HiveConf = new HiveConf()
    val jsc = new JavaSparkContext(spark.sparkContext)
    val hadoopConf = jsc.hadoopConfiguration()
    val fs = basePath.getFileSystem(hadoopConf)
    hiveConf.addResource(fs.getConf)
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
  }


  private def buildSyncConfig(basePath: Path, parameters: Map[String, String]): HiveSyncConfig = {
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig()
    hiveSyncConfig.basePath = basePath.toString
    hiveSyncConfig.baseFileFormat = parameters(HIVE_BASE_FILE_FORMAT_OPT_KEY);
    hiveSyncConfig.usePreApacheInputFormat =
      parameters.get(HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY).exists(r => r.toBoolean)
    hiveSyncConfig.databaseName = parameters(HIVE_DATABASE_OPT_KEY)
    hiveSyncConfig.tableName = parameters(HIVE_TABLE_OPT_KEY)
    hiveSyncConfig.hiveUser = parameters(HIVE_USER_OPT_KEY)
    hiveSyncConfig.hivePass = parameters(HIVE_PASS_OPT_KEY)
    hiveSyncConfig.jdbcUrl = parameters(HIVE_URL_OPT_KEY)
    hiveSyncConfig.partitionFields =
      ListBuffer(parameters(HIVE_PARTITION_FIELDS_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty).toList: _*).asJava
    hiveSyncConfig.partitionValueExtractorClass = parameters(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    hiveSyncConfig.useJdbc = parameters(HIVE_USE_JDBC_OPT_KEY).toBoolean
    hiveSyncConfig
  }

}
