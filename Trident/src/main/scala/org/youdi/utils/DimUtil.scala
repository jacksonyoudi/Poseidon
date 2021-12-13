package org.youdi.utils

import com.alibaba.fastjson.{JSON, JSONObject, parser}
import org.youdi.bean.OrderInfo
import org.youdi.common.TridentConfig
import redis.clients.jedis.Jedis

import java.sql.{Connection, DriverManager}


/**
 * redis
 * 1. 存储什么类型
 * json
 *
 * 2. 使用什么类型?
 * string set
 * 3. rediskey
 * string: table_name:id
 *
 * 不选hbase 原因
 *  1. 用户维度数据量大,冗余
 *     2. 设置过期时间
 */
object DimUtil {
  def getDimInfo(connection: Connection, tableName: String, id: String): JSONObject = {
    // 查询phoenix之前,先查询redis
    val jedis: Jedis = RedisUtil.getJedis
    val redisKey: String = "DIM:" + tableName + ":" + id
    val dimInfoJson: String = jedis.get(redisKey)

    if (dimInfoJson != null) {
      // 重置过期时间
      jedis.expire(redisKey, 24 * 60 * 60L)
      jedis.close()
      return JSON.parseObject(dimInfoJson)
    }


    val sql: String = "select * from " + TridentConfig.Hbase_SCHEMA + "." + tableName + " where id= '" + id + "'"


    val js: JSONObject = new JSONObject()
    // 查询phoenix
    val objects: List[JSONObject] = JDBCUtil.queryList(connection, sql, classOf[JSONObject])


    val result: JSONObject = objects(1)

    // 写入redis
    jedis.set(redisKey, dimInfoJson.toString)
    jedis.expire(redisKey, 24 * 60 * 60L)
    jedis.close()

    result

  }


  def delRedisDimInfo(tableName: String, id: String): Unit = {
    val jedis: Jedis = RedisUtil.getJedis
    val redisKey: String = "DIM:" + tableName + ":" + id
    jedis.del(redisKey)
    jedis.close()
  }


  def main(args: Array[String]): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)

    val connection: Connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)
    println(getDimInfo(connection, "dim_user_info", "143"))


    connection.close()

  }
}