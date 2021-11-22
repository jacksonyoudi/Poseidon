package org.youdi.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.youdi.utils.KafkaUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.time.Time

import java.text.SimpleDateFormat

object UniqueVisitApp {
  def main(args: Array[String]): Unit = {
    // 1. 环境
    // 获取配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)  // 和kafka分区保持一致

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/dwm_unique_visit/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3))

    // 2. 读取kafka数据
    val sourceTopic: String = "dwd_page_log"
    val groupid: String = "unique_visit_app"
    val sinkTopic: String = "dwm_unique_visit"
    val kafkaDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(sourceTopic, groupid))

    // 3. 将每行数据转换成json对象
    val jsonDS: DataStream[JSONObject] = kafkaDS.map(JSON.parseObject(_))




    // {"common":{"ar":"230000","uid":"18","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone Xs Max",
    //"mid":"mid_18","vc":"v2.1.134","ba":"iPhone"},
    //"page":{"page_id":"good_list","item":"苹果手机","during_time":12144,"item_type":"keyword","last_page_id":"search"},
    //"displays":[{"display_type":"query","item":"1","item_type":"sku_id","pos_id":2,"order":1},
    //{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":1,"order":2},
    //{"display_type":"query","item":"8","item_type":"sku_id","pos_id":2,"order":3},
    //{"display_type":"query","item":"4","item_type":"sku_id","pos_id":1,"order":4}],"ts":1638260449000}

    //  4. 过滤数据 状态编程 只保留每个mid每天一次登录的数据
    val keyedStream: KeyedStream[JSONObject, String] = jsonDS.keyBy(
      jsonobj => {
        jsonobj.getJSONObject("common").getString("mid")
      }
    )

    val uvDS: DataStream[JSONObject] = keyedStream.filter(new RichFilterFunction[JSONObject]() {
      var dateState: ValueState[String] = _
      var simpleDateFormat: SimpleDateFormat = _


      override def open(parameters: Configuration): Unit = {
        val desc: ValueStateDescriptor[String] = new ValueStateDescriptor[String]("date-state", classOf[String])
        // 构造者模式
        val ttlConfig: StateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
          .setUpdateType(UpdateType.OnCreateAndWrite)
          .build()


        // 设置ttl
        desc.enableTimeToLive(ttlConfig)

        dateState = getRuntimeContext.getState(desc)
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")


      }

      override def filter(value: JSONObject): Boolean = {
        // 取出上一条页面信息
        val lastPageId: String = value.getJSONObject("page").getString("last_page_id")
        // 判断上一个跳页面是否为null
        if (lastPageId == null || lastPageId.length <= 0) {
          // 取出状态数据
          val lastDate: String = dateState.value
          // 取出日期
          val curDate: String = simpleDateFormat.format(value.getLong("ts"))

          if (!curDate.equals(lastDate)) {
            dateState.update(curDate)
            return true
          }
        }
        false
      }
    })


    uvDS.print("uvDS:")

    // 5. 将数据写入 kafka

    uvDS
      .map(_.toJSONString)
      .addSink(KafkaUtils.getKafkaProducer(sinkTopic))
    // 6. 启动任务

    env.execute("dwm_unique_visit")


  }
}
