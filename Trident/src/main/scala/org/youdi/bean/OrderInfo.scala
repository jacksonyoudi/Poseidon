package org.youdi.bean

case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      total_amount: Double,
                      activity_reduce_amount: Double,
                      coupon_reduce_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      create_date: String,
                      create_hour: String,
                      create_ts: Long
                    )
