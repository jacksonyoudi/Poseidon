package org.youdi.app.function

import com.alibaba.fastjson.JSONObject
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema
import io.debezium.data.Envelope
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.language.postfixOps


class CDCDeserialization extends DebeziumDeserializationSchema[String] {
  override def deserialize(sourceRecord: SourceRecord, collector: Collector[String]): Unit = {
    // 创建json对象
    val result: JSONObject = new JSONObject()

    print(sourceRecord.toString)

    // 获取库名和表名
    val topic: String = sourceRecord.topic
    val strings: Array[String] = topic.split("\\.")


    val (database, table): (String, String) = (strings(0), strings(1))

    val value: Struct = sourceRecord.value().asInstanceOf[Struct]
    // 获取 before数据
    val before: Struct = value.getStruct("before")

    val beforJson: JSONObject = new JSONObject()
    if (before != null) {
      val beforSchema: Schema = before.schema()
      val beforefields: List[Field] = beforSchema.fields().asScala.toList


      for (field <- beforefields) {
        val v: AnyRef = before.get(field)
        beforJson.put(field.name(), v)
      }
    }
    // 获取after
    val after: Struct = value.getStruct("after")
    val afterJson: JSONObject = new JSONObject()
    if (after != null) {
      val afterSchema: Schema = after.schema()
      val afterfields: List[Field] = afterSchema.fields().asScala.toList

      for (field <- afterfields) {
        val v: AnyRef = after.get(field)
        afterJson.put(field.name(), v)
      }
    }
    // 获取操作类型
    val operation: Envelope.Operation = Envelope.operationFor(sourceRecord)
    var typ: String = operation.toString.toLowerCase

    if (typ != "create") {
      typ = "insert"
    }


    result.put("database", database)
    result.put("table", table)
    result.put("before", beforJson)
    result.put("after", afterJson)
    result.put("type", typ)

    collector.collect(result.toString)

  }

  override def getProducedType: TypeInformation[String] = {
    BasicTypeInfo.STRING_TYPE_INFO
  }
}
