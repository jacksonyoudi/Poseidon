package org.youdi.utils

import java.util.concurrent.{LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

object ThreadPoolUtil {
  var threadPoolExecutor: ThreadPoolExecutor = _


  // 等队列满了,才会创建新资源
  def getThreadPool: ThreadPoolExecutor = {
    this.synchronized(
      if (threadPoolExecutor == null) {
        threadPoolExecutor = new ThreadPoolExecutor(
          8, 16, 60L, TimeUnit.MINUTES,
          new LinkedBlockingDeque()
        )
      }
    )
    threadPoolExecutor
  }

}
