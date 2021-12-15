package org.youdi.bean


//  订单表和订单明细表 + 所需要的维度表(字段去重)
case class OrderWide(
                      var detail_id: Long = 0L,
                      var order_id: Long = 0L,
                      var sku_id: Long = 0L,
                      var order_price: Double = 0D,
                      var sku_num: Long = 0L,
                      var sku_name: String = null,
                      var split_total_amount: Double = 0D,
                      var split_activity_amount: Double = 0D,
                      var split_coupon_amount: Double = 0D,

                      var province_id: Long = 0L,
                      var order_status: String = null,
                      var user_id: Long = 0L,
                      var total_amount: Double = 0D,
                      var activity_reduce_amount: Double = 0D,
                      var coupon_reduce_amount: Double = 0D,
                      var original_total_amount: Double = 0D,
                      var feight_fee: Double = 0D,
                      var feight_fee_reduce: Double = 0D,
                      var expire_time: String = null,
                      var refundable_time: String = null,
                      var create_time: String = null,
                      var operate_time: String = null,
                      var create_date: String = null,
                      var create_hour: String = null,

                      var province_name: String = null,
                      var province_area_code: String = null,
                      var province_3166_2_code: String = null,
                      var province_iso_code: String = null,


                      var user_age: Int = 0,
                      var user_gender: String = null,

                      var spu_id: Long = null,
                      var tm_id: Long = null,
                      var category3_id: Long = null,
                      var spu_name: String = null,
                      var tm_name: String = null,
                      var category3_name: String = null,
                    ) {
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  //  def apply(orderInfo: OrderInfo, orderDetail: OrderDetail): Unit = {
  //    this(orderInfo, orderDetail)
  //  }

  def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
    if (orderInfo != null) {

      MyBeanUtils.copyProperties(orderInfo, this)

      //   BeanUtils.copyProperties(this,orderInfo)
      this.order_id = orderInfo.id
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
    if (orderDetail != null) {
      MyBeanUtils.copyProperties(orderDetail, this)
      this.detail_id = orderDetail.id
    }
  }
}