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



## FlinkCDC
datastream:
   优点： 多库多表
   缺点：  需要自定义反序列化器

flinkcdc：
   优点： 不需要自定义反序列化器
   缺点： 单表查询


## 同步工具对比


| 工具名 | flinkcdc | maxwell |  canal |
| ----      | ----       |  ----|     ---- |
|sql->数据|无|无|一对一|
|初始化功能|有多库多表|有单表|无|
|断点续传|ck|mysql|本地存储|
|封装格式|自定义|json|json(c/s自定义)|
|高可用|运行集群高可用|无|集群zk|





## DWD
侧输出流



kafka操作 

```shell
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create  --replication-factor 1 --partitions 1 --topic  dwd_start_log


./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic ods_base_db

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic ods_base_db


```


```json
{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_8","os":"iOS 13.2.3","uid":"27","vc":"v2.1.134"},"start":{"entry":"icon","loading_time":5151,"open_ad_id":11,"open_ad_ms":6184,"open_ad_skip_ms":5848},"ts":1638203457000}


{"common":{"ar":"310000","ba":"Xiaomi","ch":"xiaomi","is_new":"0","md":"Xiaomi 9","mid":"mid_11","os":"Android 8.1","uid":"40","vc":"v2.1.134"},"page":{"during_time":5195,"last_page_id":"good_detail","page_id":"cart"},"ts":1638203459000}


{"actions":[{"action_id":"trade_add_address","ts":1638203464133}],"common":{"ar":"310000","ba":"Xiaomi","ch":"xiaomi","is_new":"0","md":"Xiaomi 9","mid":"mid_11","os":"Android 8.1","uid":"40","vc":"v2.1.134"},"page":{"during_time":10267,"item":"1,2,9","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},"ts":1638203459000}


{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_9","os":"iOS 13.3.1","uid":"41","vc":"v2.1.134"},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","order":1,"pos_id":2},{"display_type":"query","item":"3","item_type":"sku_id","order":2,"pos_id":2},{"display_type":"recommend","item":"9","item_type":"sku_id","order":3,"pos_id":2}],"page":{"during_time":1044,"page_id":"home"},"ts":1638203458000}
```



## 动态分流

维度表：
hbase:  数据量扩展大， 请求的并发， 一致性
redis:  
mysql： 并发压力大



配置：
1. 使用zk, 通过watch感知数据变化
2. 使用mysql， 周期性同步
3. 用mysql保存，使用广播流



## 配置表
table_process字段

sourceTable type sinktype sink(表名和主题) sinkColumns pk  extend
order_info  insert  kafka   dwd_xxa       


```sql



```

广播流：
   1. 解析数据 string => table_process
   2. 检查hbase表是否存在并建表
   3. 写入状态

主流：
   1. 读取状态
   2. 过滤数据
   3. 分流



### ODS和DWD-DIM总结
ODS:
   数据源： 行为数据， 业务数据
   架构分析：
      flinkCDC: sql, datastream
            maxwell,cancel
   保持数据原貌，不做任何修改
   ods_base_log, ods_base_db

DWD-DIM:
   行为数据:DWD(KAFKA)
         1. 过滤脏数据， -->  侧输出流， 脏数据率
         2. 新老用户校验  --> 前台校验不准
         3. 分流  ---> 侧输出流  页面，启动，曝光，动作， 错误
         4. 写入kafka

   业务数据: DWD(kAFKA) -DIM(HBASE)
         1. 过滤数据 --> 删除数据





   








   



