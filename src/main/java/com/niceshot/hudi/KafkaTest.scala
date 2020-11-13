package com.niceshot.hudi

import com.niceshot.hudi.util.CanalDataProcessor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * @author created by chenjun at 2020-10-23 17:18
 *
 */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(60))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.16.237:9092,192.168.16.236:9092",
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream2",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //stream.map(record => (record.key, record.value)).print(6)
    stream.map(record => {
      //val obj = CanalDataParser.parse(record.value())
      println("hello")
    }).count().print();
    ssc.start()
    ssc.awaitTermination()
  }
}
