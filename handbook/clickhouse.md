# ClickHouse

1. 列式存储DBMS
    - 聚合
    - 压缩率高
    
OLAP： 在线分析 
OLTP: 在线事务处理





4.4.1 分区


先写入一个临时目录中

bin 字段
idx 索引

### 主键




## OLAP 

rollup 上卷

group by  a,b with rollup 


cube
所有维度组合都计算出来


total:

group by a, b  和 sum 


## 副本

只能同步数据， 不能同步表结构


```shell
ReplicatedMergeTree(zk,node_name)
```


## 切片

元数据表 

internal_replication 


errors_count 


## 分布式表
distributed 
