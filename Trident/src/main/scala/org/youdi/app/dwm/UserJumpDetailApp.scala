package org.youdi.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.youdi.utils.KafkaUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

object UserJumpDetailApp {
  def main(args: Array[String]): Unit = {
    // 1. 环境配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 和kafka分区保持一致

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/UserJumpDetailApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3))

    // 2. 读取kafka数据
    val sourceTopic: String = "dwd_page_log"
    val groupid: String = "UserJumpDetailApp"
    val sinkTopic: String = "dwm_user_jump_detail"
    val kafkaDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(sourceTopic, groupid))

    // 3. 将每行数据转换成json对象并提取时间戳生成watemark
    val jsonobjDS: DataStream[JSONObject] = kafkaDS
      .map(JSON.parseObject(_))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[JSONObject](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
          override def extractTimestamp(element: JSONObject, recordTimestamp: Long): Long = {
            element.getLong("ts")
          }
        })
      )
    // 4. 定义模式序列
    val pattern: Pattern[JSONObject, JSONObject] = Pattern.begin[JSONObject]("start").where(new SimpleCondition[JSONObject] {
      override def filter(value: JSONObject) = {
        val lastPageId: String = value.getJSONObject("page").getString("last_page_id")
        lastPageId == null || lastPageId.length <= 0
      }
    })
      .next("next")
      .where(new SimpleCondition[JSONObject] {
        override def filter(value: JSONObject) = {
          val lastPageId: String = value.getJSONObject("page").getString("last_page_id")
          lastPageId == null || lastPageId.length <= 0
        }
      })
      .within(
        Time.seconds(10)
      )

    // 5. 将模式序列作用到流上
    val patternStream: PatternStream[JSONObject] = CEP.pattern(jsonobjDS.keyBy(_.getJSONObject("common").getString("mid")), pattern)

    // 6. 提取匹配上的超时事件
    val timeoutTag: OutputTag[JSONObject] = new OutputTag[JSONObject]("timeout")


    val selectDS: DataStream[JSONObject] = patternStream.select(
      timeoutTag,
      new PatternTimeoutFunction[JSONObject, JSONObject]() {
        override def timeout(pattern: util.Map[String, util.List[JSONObject]], timeoutTimestamp: Long): JSONObject = {
          pattern.get("start").get(0)
        }
      },

      new PatternSelectFunction[JSONObject, JSONObject]() {
        override def select(pattern: util.Map[String, util.List[JSONObject]]) = {
          pattern.get("start").get(0)
        }
      }
    )

    val timeOutDS: DataStream[JSONObject] = selectDS.getSideOutput(timeoutTag)

    // 7. union两种事件
    val unionDS: DataStream[JSONObject] = selectDS.union(timeOutDS)

    // 将数据写到kafka
    unionDS.print("jump:")
    unionDS.map(_.toJSONString)
      .addSink(KafkaUtils.getKafkaProducer("sinkTopic"))

    // 启动
    env.execute(this.getClass.getName)

  }
}
