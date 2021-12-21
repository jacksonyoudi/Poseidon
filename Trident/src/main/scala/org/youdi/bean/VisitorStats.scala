package org.youdi.bean

case class VisitorStats(
                         //统计开始时间
                         var stt: String = null,
                         //统计结束时间
                         var edt: String = null,
                         //维度：版本
                         var vc: String = null,
                         //维度：渠道
                         var ch: String = null,
                         //维度：地区
                         var ar: String = null,
                         //维度：新老用户标识
                         var is_new: String = null,
                         //度量：独立访客数
                         var uv_ct: Long = 0L,
                         //度量：页面访问数
                         var pv_ct: Long = 0L,
                         //度量： 进入次数
                         var sv_ct: Long = 0L,
                         //度量： 跳出次数
                         var uj_ct: Long = 0L,
                         //度量： 持续访问时间
                         var dur_sum: Long = 0L,
                         //统计时间
                         var ts: Long = 0L
                       )
