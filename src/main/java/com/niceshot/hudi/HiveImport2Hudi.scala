package com.niceshot.hudi

import com.niceshot.hudi.config.{CanalKafkaImport2HudiConfig, HiveImport2HudiConfig}
import com.niceshot.hudi.constant.Constants
import com.niceshot.hudi.util.ConfigParser
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author created by chenjun at 2020-11-02 10:49
 *
 */
object HiveImport2Hudi {
  def main(args: Array[String]): Unit = {
    val config = ConfigParser.parseHiveImport2HudiConfig(args)
    val spark = SparkSession
      .builder()
      .appName("hive_2_hudi_"+config.getSyncHiveDb+"_"+config.getSyncHiveTable)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", config.getHiveBasePath)
      .enableHiveSupport()
      .master("local[3]")
      .getOrCreate()
    val df = spark.sqlContext.sql("""SELECT result.*,date_format(to_timestamp("""+config.getCreateTimeStampKey+""", "yyyy-MM-dd HH:mm:ss"), "yyyy/MM/dd") as """+ Constants.HudiTableMeta.PARTITION_KEY+""" from """+config.getSyncHiveDb+"""."""+config.getSyncHiveTable+""" as result""")
    df.show()
    hudiDataUpsert(config,df)
  }

  private def hudiDataUpsert(config: HiveImport2HudiConfig,data:DataFrame): Unit = {
    data.write.format("hudi").
      option(OPERATION_OPT_KEY,UPSERT_OPERATION_OPT_VAL).
      option(PRECOMBINE_FIELD_OPT_KEY,config.getPrecombineKey).
      option(RECORDKEY_FIELD_OPT_KEY, config.getPrimaryKey).
      option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
      option(TABLE_NAME, config.getStoreTableName).
      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY,true).
      mode(SaveMode.Append).
      save(config.getRealSavePath)
  }
}
