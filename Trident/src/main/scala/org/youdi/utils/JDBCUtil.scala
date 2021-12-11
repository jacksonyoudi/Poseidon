package org.youdi.utils

import com.alibaba.fastjson.JSONObject
import com.google.common.base.CaseFormat
import org.apache.commons.beanutils.BeanUtils
import org.youdi.common.TridentConfig

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import scala.collection.immutable.List


object JDBCUtil {
  def queryList[T](connection: Connection, querySql: String, clz: Class[T], underScoreToCamel: Boolean = false): List[T] = {
    val list: List[T] = List[T]()
    val statement: PreparedStatement = connection.prepareStatement(querySql)
    val set: ResultSet = statement.executeQuery()
    val metaData: ResultSetMetaData = set.getMetaData
    val columnCount: Int = metaData.getColumnCount


    while (set.next()) {
      // 创建泛型对象并赋值
      val t: T = clz.newInstance()
      // 给赋值
      for (i <- 1 to columnCount) {
        // 获取列名
        var columnName: String = metaData.getColumnName(i)

        // 判断是否转换为驼峰命名
        if (underScoreToCamel) {
          columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName)
        }


        val value: AnyRef = set.getObject(i)
        BeanUtils.setProperty(t, columnName, value)
      }
      list.+:(t)
    }

    set.close()
    statement.close()
    list
  }







  def main(args: Array[String]): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)
    val connection: Connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)

    val list: List[JSONObject] = queryList(connection, "select * from dim_user_info", classOf[JSONObject], true)

    for (i <- list) {
      println(i)
    }

    connection.close()
  }
}
