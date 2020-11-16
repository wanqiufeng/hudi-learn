package com.niceshot.hudi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author created by chenjun at 2020-10-14 15:32
 *
 */
object SparkQueryHudiTable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate();
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(args.apply(0))
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    println("first query===========>")
    spark.sql(args.apply(1)).show()
  }
}
