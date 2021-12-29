package com.youdi.streamlet.tablewithsql.kafka

import org.apache.flink.core.fs.FileSystem

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink

object KafkaTableConnector {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    tabEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("ods-order")
        .startFromEarliest()
        .property("zookeeper.connect", "172.27.133.19:7181")
        .property("bootstrap.servers", "172.27.133.19:9092")
    ).withFormat(
      new Json().jsonSchema(
        "{" + "  \"definitions\": {" + "    \"orderItems\": {" + "      \"type\": \"object\"," + "      \"properties\": {" + "        \"orderItemId\": {" + "          \"type\": \"string\"" + "        }," + "        \"price\": {" + "          \"type\": \"number\"" + "        }," + "        \"itemLinkIds\": {" + "          \"type\": \"object\"," + "          \"properties\": {" + "            \"linkId\": {" + "              \"type\": \"string\"" + "            }," + "            \"price\": {" + "              \"type\": \"number\"" + "            }" + "          }," + "          \"required\": [" + "            \"linkId\"," + "            \"price\"" + "          ]" + "        }" + "      }," + "      \"required\": [" + "        \"orderItemId\"," + "        \"price\"" + "      ]" + "    }" + "  }," + "  \"type\": \"object\"," + "  \"properties\": {" + "    \"transactionId\": {" + "      \"type\": \"string\"" + "    }," + "    \"orderTime\": {" + "      \"type\": \"string\"," + "      \"format\": \"date-time\"" + "    }," + "    \"orderItems\": {" + "      \"oneOf\": [" + "        {" + "          \"type\": \"null\"" + "        }," + "        {" + "          \"$ref\": \"#/definitions/orderItems\"" + "        }" + "      ]" + "    }" + "  }" + "}"
      )
    ).withSchema(
      new Schema().field("transactionId", Types.STRING)
        .field("orderTime", Types.SQL_TIMESTAMP)
        .field("promiseTime", Types.SQL_TIMESTAMP)
    ).inAppendMode().createTemporaryTable("order_table")

    val path :String = "a.csv"
    val sink = new CsvTableSink(
      path, "|", 1, FileSystem.WriteMode.OVERWRITE
    )
  }
}
