package org.youdi.bean


import scala.beans.BeanProperty

case class OrderDetail(
                        @BeanProperty var  id: Long,
                        @BeanProperty var order_id: Long,
                        @BeanProperty var sku_id: Long,
                        @BeanProperty var order_price: Double,
                        @BeanProperty var sku_num: Long,
                        @BeanProperty var sku_name: String,
                        @BeanProperty var create_time: String,
                        @BeanProperty var split_total_amount: Double,
                        @BeanProperty var split_activity_amount: Double,
                        @BeanProperty var split_coupon_amount: Double,
                        @BeanProperty var create_ts: Long
                      )
