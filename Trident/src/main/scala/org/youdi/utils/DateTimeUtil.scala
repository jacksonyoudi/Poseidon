package org.youdi.utils

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date


object DateTimeUtil {
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def toYMDhms(date: Date): String = {
    val ts: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
    formatter.format(ts)
  }

  def toTs(YmDHms: String): Long = {
    val time: LocalDateTime = LocalDateTime.parse(YmDHms, formatter)
    time.toInstant(ZoneOffset.of("+8")).toEpochMilli
  }

}
