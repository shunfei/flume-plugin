# flume 插件
## 1. flume介绍
[apache flume](http://flume.apache.org/)是一个传输工具，[舜飞科技](http://www.sunteng.com)数据中心使用flume进行实时数据传输，传输数据量10000 events/s，10M/s，上线一年没挂过，非常稳定

舜飞使用flume主要有以下场景：

1. **file->kafka**：使用sunteng-tail-source和sunteng-kafka-channel，不需要sink
2. **kafka->hdfs**：使用sunteng-kafka-channel和hdfs-sink，不需要source 

## 2. 插件介绍

- tail-source 
  - 支持断点续传
  - 不修改原文件的元数据信息
  - 支持多级目录*号匹配
- kafka-channel
  - 修改自官方的kafka-channel
  - 去掉header存储，去掉avro序列化，保证kafka里面就是可读数据


## 4. 作者

#### [**@chzyer**](https://github.com/chzyer)