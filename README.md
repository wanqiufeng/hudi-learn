
##### 同步历史数据至hudi表
这里采用的思路是
- 将mysql全量数据通过注入sqoop等工具，导入到hive表。
- 然后采用分支代码中的工具HiveImport2HudiConfig，将数据导入Hudi表

HiveImport2HudiConfig提供了如下一些参数，用于配置程序执行行为

| 参数名      |     含义 |   是否必填   |默认值|
| :-------- | --------:| :------: | :------: |
| `--base-save-path`|   hudi表存放在HDFS的基础路径，比如hdfs://192.168.16.181:8020/hudi_data/|  是|无|
| `--mapping-mysql-db-name`|   指定处理的Mysql库名|  是|无|
| `--mapping-mysql-table-name`|   指定处理的Mysql表名|  是|无|
| `--store-table-name`|   指定Hudi的表名|  否|默认会根据--mapping-mysql-db-name和--mapping-mysql-table-name自动生成。假设--mapping-mysql-db-name 为crm，--mapping-mysql-table-name为order。那么最终的hudi表名为crm__order|
| `--real-save-path`|  指定hudi表最终存储的hdfs路径|  否|默认根据--base-save-path和--store-table-name自动生成，生成格式为'--base-save-path'+'/'+'--store-table-name' ，推荐默认|
| `--primary-key`|  指定同步的hive历史表中能唯一标识记录的字段名|  否|默认id|
| `--partition-key`|  指定hive历史表中可以用于分区的时间字段，字段必须是timestamp 或dateime类型|  是|无|
| `--precombine-key`|  最终用于配置hudi的`hoodie.datasource.write.precombine.field`|  否|默认id|
| `--sync-hive-db-name`|  全量历史数据所在hive的库名|  是|无|
| `--sync-hive-table-name`| 全量历史数据所在hive的表名|  是|无|
| `--hive-base-path`|  hive的所有数据文件存放地址，需要参看具体的hive配置|  否|/user/hive/warehouse|
| `--hive-site-path`| hive-site.xml配置文件所在的地址| 是|无|
| `--tmp-data-path`|  程序执行过程中临时文件存放路径。一般默认路径是/tmp。有可能出现/tmp所在磁盘太小，而导致历史程序执行失败的情况。当出现该情况时，可以通过该参数自定义执行路径|  否|默认操作系统临时目录|

一个程序执行demo

```
nohup java -jar hudi-learn-1.0-SNAPSHOT.jar --sync-hive-db-name hudi_temp --sync-hive-table-name crm__wx_user_info --base-save-path hdfs://192.168.2.2:8020/hudi_table/ --mapping-mysql-db-name crm --mapping-mysql-table-name "order" --primary-key "id" --partition-key created_date --hive-site-path /etc/lib/hive/conf/hive-site.xml --tmp-data-path /data/tmp > order.log &
```


##### 同步hudi表结构至hive meta
需要将hudi的数据结构和分区，以hive外表的形式同步至Hive meta，才能是Hive感知到hudi数据，并通过sql进行查询分析。Hudi本身在消费Binlog进行存储时，可以顺带将相关表元数据信息同步至hive。但考虑到数据到每条Hudi数据写入都要读写Hive Meta ，对Hive的性能可能影响很大。所以我单独开发了HiveMetaSyncConfig工具，用于同步hudi表元数据至Hive。考虑到目前程序只支持按天分区，所以同步工具可以一天执行一次即可。参数配置如下

| 参数名      |     含义 |   是否必填   |默认值|
| :-------- | --------:| :------: |:------: |
| `--hive-db-name`|   指定hudi表同步至哪个hive数据库|  是| 无  |
| `--hive-table-name`|   指定hudi表同步至哪个hive表 |  是  |无  |
| `--hive-jdbc-url`|   指定hive meta的jdbc链接地址，例如jdbc:hive2://192.168.16.181:10000|  是|无  |
| `--hive-user-name`|   指定hive meta的链接用户名|  否  |默认hive |
| `--hive-pwd`|   指定hive meta的链接密码 |  否  |默认hive  |
| `--hudi-table-path`|   指定hudi表所在hdfs的文件路径|  是  |无  |
| `--hive-site-path`|   指定hive的hive-site.xml路径| 是  |无  |

一个程序执行demo

```
java -jar hudi-learn-1.0-SNAPSHOT.jar --hive-db-name streaming --hive-table-name crm__order --hive-user-name hive --hive-pwd hive --hive-jdbc-url jdbc:hive2://192.168.16.181:10000 --hudi-table-path hdfs://192.168.16.181:8020/hudi_table/crm__order --hive-site-path /lib/hive/conf/hive-site.xml
```
