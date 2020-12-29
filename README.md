# 项目简介
本项目是使用Spark 对Apache Hudi进行封装，实现了消费Mysql binlog 将Mysql表映射至Hive表
Mysql binlog的采集方式使用阿里的 canal 实现
# Apache hudi 整体介绍文档地址
https://www.cnblogs.com/niceshot/p/14198360.html

# 工具简介
不要使用主分支代码，按需使用如下两个分支
- [binlog-consumer](https://github.com/wanqiufeng/hudi-learn/tree/binlog-consumer) 分支是消费Kafka binlog相关的代码
- [history_import_and_meta_sync](https://github.com/wanqiufeng/hudi-learn/tree/history_import_and_meta_sync) 分支提供了历史数据导入，以及hudi表结构同步至Hive meta的工具实现
