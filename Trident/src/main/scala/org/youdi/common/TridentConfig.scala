package org.youdi.common


object TridentConfig {
  val Hbase_SCHEMA: String = "Poseidon"
  val PHOENIX_DRIVER: String = "org.apache.phoenix.jdbc.PhoenixDriver"
  val PHOENIX_SERVER: String = "jdbc:phoenix:192.168.1.100:2181/hbase"

  val CLICKHOUSE_URL: String = "jdbc:clickhouse://127.0.0.1:8123/trident"
  var CLICKHOUSE_DRIVER: String = "ru.yandex.clickhouse.ClickHouseDriver"

}
