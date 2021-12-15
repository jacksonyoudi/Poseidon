package org.youdi.bean

case class PaymentInfo(
                        var id: Long = 0L,
                        var order_id: Long = 0L,
                        var use_id: Long = 0L,
                        var total_amount: Double = 0D,
                        var subject: String = null,
                        var payment_type: String = null,
                        var create_time: String = null,
                        var callback_time: String = null
                      )
