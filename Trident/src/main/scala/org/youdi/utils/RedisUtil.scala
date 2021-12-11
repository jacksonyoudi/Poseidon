package org.youdi.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool: JedisPool = _

  def getJedis: Jedis = {
    if (jedisPool == null) {
      val config: JedisPoolConfig = new JedisPoolConfig()
      config.setMaxTotal(100)
      config.setBlockWhenExhausted(true)
      config.setMaxWaitMillis(2000)
      config.setMaxIdle(5)
      config.setMinIdle(5)
      config.setTestOnBorrow(true)

      jedisPool = new JedisPool(config, "127.0.0.1", 6379, 1000)

      println("开辟连接池")
    }
    jedisPool.getResource
  }

}
