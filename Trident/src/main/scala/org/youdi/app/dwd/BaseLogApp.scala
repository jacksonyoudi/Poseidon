package org.youdi.app.dwd

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.youdi.utils.KafkaUtils
import org.apache.flink.api.common.time.Time

import scala.language.postfixOps

// 数据流  web/app -> nginx -> springboot -> kafka -> flinkapp -> kafka

object BaseLogApp {
  def main(args: Array[String]): Unit = {
    // 获取配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/BaseLogApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)

    // 消费ods_base_log 主题数据创建流
    val sourceTopic: String = "ods_base_log"
    val groupid: String = "base_log_app"
    val odsSource: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(sourceTopic, groupid))
    // 将每行数据转换成json map

    val dirtyTag: OutputTag[String] = new OutputTag[String]("Dirty") {}


    val jsonDs: DataStream[JSONObject] = odsSource.process(
      new ProcessFunction[String, JSONObject](

      ) {
        override def processElement(value: String, ctx: ProcessFunction[String, JSONObject]#Context, out: Collector[JSONObject]) = {
          try {
            val jsonObject: JSONObject = JSON.parseObject(value)
            out.collect(jsonObject)
          } catch {
            case ex: Exception => {
              // 发生异常，写入侧输出流
              ctx.output(dirtyTag, value)
            }
          }
        }
      }
    )
    // 新老用户校验，状态编程
    val fixedDS: DataStream[JSONObject] = jsonDs
      .keyBy(_.getJSONObject("common").getString("mid"))
      .map(
        new RichMapFunction[JSONObject, JSONObject]() {
          private var valueState: ValueState[Int] = _


          override def open(parameters: Configuration) = {
            // 初始化状态
            val descriptor: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("value-state", classOf[Int])

            val ttlConfig: StateTtlConfig = StateTtlConfig
              .newBuilder(Time.seconds(1))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build
            descriptor.enableTimeToLive(ttlConfig)

            valueState = getRuntimeContext().getState(descriptor)
          }

          override def map(value: JSONObject) = {
            // 获取数据中的标记
            val isNeww: String = value.getJSONObject("common").getString("is_new")
            if ("1" == isNeww) {
              val state: Int = valueState.value()
              if (state != null) {
                value.getJSONObject("common").put("is_new", "0")
              } else {
                valueState.update(1);
              }
            }
            value
          }
        }
      )


    val startTag: OutputTag[String] = new OutputTag[String]("start")
    val displayTag: OutputTag[String] = new OutputTag[String]("display")


    // 分流 侧输出流， 页面  启动  曝光
    val pageDS: DataStream[String] = fixedDS.process(new ProcessFunction[JSONObject, String] {
      override def processElement(value: JSONObject, ctx: ProcessFunction[JSONObject, String]#Context, out: Collector[String]) = {
        val start: String = value.getString("start")
        if (start != null && start.length > 0) {
          ctx.output(startTag, value.toJSONString)
        } else {
          out.collect(value.toJSONString)

          // 取出曝光数据
          val displays: JSONArray = value.getJSONArray("displays")

          val pageId: String = value.getJSONObject("page").getString("page_id")

          if (displays != null && displays.size > 0) {
            for (i <- 0 until displays.size) {
              val display: JSONObject = displays.getJSONObject(i)

              // 添加页面id
              display.put("page_id", pageId)

              // 数据写到曝光侧输出流
              ctx.output(displayTag, display.toJSONString)
            }
          }
        }
      }
    })

    // 提取侧输出流
    val startDS: DataStream[String] = pageDS.getSideOutput(startTag)
    val displayDS: DataStream[String] = pageDS.getSideOutput(displayTag)
    val dirtyDS: DataStream[String] = jsonDs.getSideOutput(dirtyTag)

    // 将3个流打印并输出到对应的kafka主题中
//    startDS.print("start>>>>>>>")
    startDS.addSink(KafkaUtils.getKafkaProducer("dwd_start_log"))

//    displayDS.print("display>>>>>>>")
    displayDS.addSink(KafkaUtils.getKafkaProducer("dwd_display_log"))

//    pageDS.print("page>>>>>>>")
    pageDS.addSink(KafkaUtils.getKafkaProducer("dwd_page_log"))

    dirtyDS.print("dirty>>>>>>>")


    // 启动任务
    env.execute("BaseLogApp")

  }
}




