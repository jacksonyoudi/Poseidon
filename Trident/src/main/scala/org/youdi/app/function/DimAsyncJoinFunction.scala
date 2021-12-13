package org.youdi.app.function

import com.alibaba.fastjson.JSONObject

trait DimAsyncJoinFunction[T] {
  abstract def getKey(input: T): String

  abstract def join(intput: T, dimInfo: JSONObject)
}
