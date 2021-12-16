package org.youdi.bean

import org.apache.commons.beanutils.BeanUtils
import java.lang.reflect.InvocationTargetException


case class PaymentWide(
                        var payment_id: Long = 0L,

                        var subject: String = null,
                        var payment_type: String = null,
                        var payment_create_time: String = null,
                        var callback_time: String = null,
                        var detail_id: Long = 0L,
                        var order_id: Long = 0L,
                        var sku_id: Long = 0L,
                        var order_price: Double = null,
                        var sku_num: Long = 0L,
                        var sku_name: String = null,
                        var province_id: Long = 0L,
                        var order_status: String = null,
                        var user_id: Long = 0L,
                        var total_amount: Double = null,
                        var activity_reduce_amount: Double = null,
                        var coupon_reduce_amount: Double = null,
                        var original_total_amount: Double = null,
                        var feight_fee: Double = null,
                        var split_feight_fee: Double = null,
                        var split_activity_amount: Double = null,
                        var split_coupon_amount: Double = null,
                        var split_total_amount: Double = null,
                        var order_create_time: String = null,

                        var province_name: String = null, //查询维表得到

                        var province_area_code: String = null,
                        var province_iso_code: String = null,
                        var province_3166_2_code: String = null,

                        var user_age: Integer = null, //用户信息

                        var user_gender: String = null,

                        var spu_id: Long = 0L, //作为维度数据 要关联进来

                        var tm_id: Long = 0L,
                        var category3_id: Long = 0L,
                        var spu_name: String = null,
                        var tm_name: String = null,
                        var category3_name: String = null
                      ) {

  def this(paymentInfo: PaymentInfo, wide: OrderWide) {
    this
    mergePaymentInfo(paymentInfo)
    mergeOrderWide(wide)
  }

  def mergePaymentInfo(paymentInfo: PaymentInfo): Unit = {
    if (paymentInfo != null) try {
      BeanUtils.copyProperties(this, paymentInfo)
      payment_create_time = paymentInfo.create_time
      payment_id = paymentInfo.id
    } catch {
      case e: IllegalAccessException =>
        e.printStackTrace()
      case e: InvocationTargetException =>
        e.printStackTrace()
    }
  }


  def mergeOrderWide(orderWide: OrderWide): Unit = {
    if (orderWide != null) try {
      BeanUtils.copyProperties(this, orderWide)
      order_create_time = orderWide.create_time
    } catch {
      case e: IllegalAccessException =>
        e.printStackTrace()
      case e: InvocationTargetException =>
        e.printStackTrace()
    }
  }
}

