# 实时数仓分层介绍

ODS: 原始数据，日志和业务数据
DWD：根据数据对象为单位进行分流，比如订单，页面访问等
DIM： 维度数据(需要历史，查询)

DWM: 对于部分数据进一步加工，比如独立访问，跳出行为，也可以和维度进行关联，形成宽表，依旧是明细数据。
DWS: 根据某个主题将多个事实数据轻度聚合，形成主题宽表。

ADS: 把clickhouse中数据根据可视化需要进行筛选聚合。




离线计算-实时计算
需求的固定

即席查询： 需求的临时性

presto: 当场计算 基于内存速度快
Kylin 预计算，提前算好， 多维分析 hive with cube




# 场景
1. 实时数仓
2. 数据预警或提示
3. 实时推荐系统



# 架构
## 离线数仓架构

flume
tailDirSouce:
端点续传
监控多目录
文件更名后，数据会重复
注意： 1.使用不更名的打印日志框架 logback
2. 修改源码，让tailDirSouce判断文件是只读inode值


kafkaChannel
省去一层 sink
kafka: 生成者 消费者
用法：
1. source-kafkachannel-sink
2. source-kafkaChannel
3. kafkachannel-sink




sqoop同步数据方式
1. 增量
2. 全量
3. 新增及变化
4. 特殊 只导入一次

## 逻辑线
数据流，监控，优化，配置




# Kafka
1. 生产者
   ack, 0， 1， -1
   拦截器，序列化器，分区器， 发送流程， 事务 sender main
   幂等性 事务
   分区规则 ——》  指定分区，hash， 轮询，粘性
2. 消费者
   分区分配规则
   offset保存
   默认: __consumer_offset主题
   其他： 手动维护offset(mysql)
   保存数据 & 保存offset写到一个事务， 精准一次消费，
   先保存数据后保存offset  重复数据
   先保存offset保存数据



3. broker
   topic
   副本： 高可靠
   ISR: LEO,HW
   分区： 高并发，负载均衡， 防止热点

优化，监控， 配置， 数据量，





## flume
小文件问题

## Hive

ODS
dwd
dws
ads

1. 解析器
2. 编译器
3. 优化器
4. 执行器


## 实时架构

Canal/maxwell/FlinkCDC
ROW



### Kafka
ODS
快，减少IO， 耦合性高

行为数据， 业务数据

DWD
kafka 维度表数据 hbase

flink

clickhouse


ADS：



## 离线架构
优点：   耦合性低，稳定性高
缺点： 时效性差一点
说明：
1. 稳定性 追求
2.

实时架构：
优点： 时效性好
缺点： 耦合性高，稳定性低
1. 时效性好
2. kafka是高可用，挂一些没问题
3. 数据量
4. flink




# 日志采集

## springboot
    数据接口

    controller: 拦截用户请求，调用service，响应用户请求  服务员
    service:  调用DAO层，加工数据                      厨房
    DAO (mapper)： 存储数据                           采购   

    持久化层： 存储数据                                菜市场

优点：
分层思想
解耦
复用









   



