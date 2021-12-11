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
                      @BeanProperty var create_ts: Long,

                      @BeanProperty var province_name: String = null, //查询维表得到
                      @BeanProperty var province_area_code: String = null,
                      @BeanProperty var province_3166_2_code: String = null,
                      @BeanProperty var province_iso_code: String = null,

                      var user_age: Int = 0,
                      var user_gender: String = null
                    )
