package com.youdi.streamlet.processfunction


case class CountWithTimestamp(
                               var key: String,
                               var count: Long,
                               var lastModified: Long
                             ) {
  def this(key: String) = {
    this(key, 0, 0)
  }
}
