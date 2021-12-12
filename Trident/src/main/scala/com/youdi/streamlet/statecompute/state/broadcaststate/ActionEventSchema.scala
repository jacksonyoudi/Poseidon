package com.youdi.streamlet.statecompute.state.broadcaststate

import com.alibaba.fastjson.JSON
import com.youdi.streamlet.statecompute.state.broadcaststate.model.Action
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation

class ActionEventSchema extends DeserializationSchema[Action] with SerializationSchema[Action] {
  private val serialVersionUID: Long = 6154188370181669758L

  override def deserialize(message: Array[Byte]): Action = {
    JSON.parseObject(message, classOf[Action])
  }

  override def isEndOfStream(nextElement: Action): Boolean = {
    false
  }

  override def serialize(element: Action): Array[Byte] = {
    element.toString.getBytes
  }

  override def getProducedType: TypeInformation[Action] = {
    TypeInformation.of(classOf[Action])
  }
}
