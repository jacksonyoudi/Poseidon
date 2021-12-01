package org.youdi.bean


import scala.beans.BeanProperty

case class OrderInfo(
                      @BeanProperty var id: Long,
                      @BeanProperty var province_id: Long,
                      @BeanProperty var order_status: String,
                      @BeanProperty var total_amount: Double,
                      @BeanProperty var activity_reduce_amount: Double,
                      @BeanProperty var coupon_reduce_amount: Double,
                      @BeanProperty var feight_fee: Double,
                      @BeanProperty var expire_time: String,
                      @BeanProperty var create_time: String,
                      @BeanProperty var operate_time: String,
                      @BeanProperty var create_date: String,
                      @BeanProperty var create_hour: String,
                      @BeanProperty var create_ts: Long
                    )
