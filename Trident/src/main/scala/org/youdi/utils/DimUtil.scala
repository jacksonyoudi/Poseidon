package org.youdi.utils

import com.alibaba.fastjson.JSONObject
import org.youdi.common.TridentConfig

import java.sql.Connection


object DimUtil {
  def getDimInfo(connection: Connection, tableName: String, id: String): JSONObject = {
    val sql: String = "select * from " + TridentConfig.Hbase_SCHEMA + "." + tableName + " where id=" + id

    new JSONObject()
  }

}