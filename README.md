# 入口程序
CanalKafkaImport2Hudi
# 参数说明


| 参数名|     含义|   是否必填|默认值|
| :-------- | --------:| :------: |:------: |
| `--base-save-path`|   hudi表存放在HDFS的基础路径，比如hdfs://192.168.16.181:8020/hudi_data/|  是|无|
| `--mapping-mysql-db-name`|   指定处理的Mysql库名|  是|无|
| `--mapping-mysql-table-name`|   指定处理的Mysql表名|  是|无|
| `--store-table-name`|   指定Hudi的表名|  否|默认会根据--mapping-mysql-db-name和--mapping-mysql-table-name自动生成。假设--mapping-mysql-db-name 为crm，--mapping-mysql-table-name为order。那么最终的hudi表名为crm__order|
| `--real-save-path`|  指定hudi表最终存储的hdfs路径|  否|默认根据--base-save-path和--store-table-name自动生成，生成格式为'--base-save-path'+'/'+'--store-table-name' ，推荐默认|
| `--primary-key`|  指定同步的mysql表中能唯一标识记录的字段名|  否|默认id|
| `--partition-key`|  指定mysql表中可以用于分区的时间字段，字段必须是timestamp 或dateime类型|  是|无|
| `--precombine-key`|  最终用于配置hudi的`hoodie.datasource.write.precombine.field`|  否|默认id|
| `--kafka-server`|  指定Kafka 集群地址|  是|无|
| `--kafka-topic`|  指定消费kafka的队列|  是|无|
| `--kafka-group`|  指定消费kafka的group|  否|默认在存储表名前加'hudi'前缀，比如'hudi_crm__order'|
| `--duration-seconds`|  由于本程序使用Spark streaming开发，这里指定Spark streaming微批的时长|  否|默认10秒|

一个使用的demo如下

```
spark-2.4.4-bin-hadoop2.6/bin/spark-submit --class com.niceshot.hudi.CanalKafkaImport2Hudi \
	--name hudi__goods \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
	--num-executors 1 \
    --queue hudi \
    --conf spark.executor.memoryOverhead=2048 \
    --conf "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=\tmp\hudi-debug" \
	--conf spark.core.connection.ack.wait.timeout=300 \
	--conf spark.locality.wait=100 \
	--conf spark.streaming.backpressure.enabled=true \
	--conf spark.streaming.receiver.maxRate=500 \
	--conf spark.streaming.kafka.maxRatePerPartition=200 \
	--conf spark.ui.retainedJobs=10 \
	--conf spark.ui.retainedStages=10 \
	--conf spark.ui.retainedTasks=10 \
	--conf spark.worker.ui.retainedExecutors=10 \
	--conf spark.worker.ui.retainedDrivers=10 \
	--conf spark.sql.ui.retainedExecutions=10 \
	--conf spark.yarn.submit.waitAppCompletion=false \
	--conf spark.yarn.maxAppAttempts=4 \
	--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
	--conf spark.yarn.max.executor.failures=20 \
	--conf spark.yarn.executor.failuresValidityInterval=1h \
	--conf spark.task.maxFailures=8 \
    hudi-canal-import-1.0-SNAPSHOT-jar-with-dependencies.jar  --kafka-server local:9092 --kafka-topic dt_streaming_canal_xxx --base-save-path hdfs://192.168.2.1:8020/hudi_table/ --mapping-mysql-db-name crm --mapping-mysql-table-name order --primary-key id --partition-key createDate --duration-seconds 1200
```
