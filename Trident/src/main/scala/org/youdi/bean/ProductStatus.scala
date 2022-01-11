package org.youdi.bean


/**
 * Desc: 商品统计实体类
 *
 * @Builder注解 可以使用构造者方式创建对象，给属性赋值
 * @Builder.Default 在使用构造者方式给属性赋值的时候，属性的初始值会丢失
 *                  该注解的作用就是修复这个问题
 *                  例如：我们在属性上赋值了初始值为0L，如果不加这个注解，通过构造者创建的对象属性值会变为null
 */

import lombok.Builder
import scala.collection.immutable.HashSet

case class ProductStatus(


                          var stt: String = null, //窗口起始时间

                          var edt: String = null, //窗口结束时间

                          var sku_id: Long = 0L, //sku编号

                          var sku_name: String = null, //sku名称

                          var sku_price: Double = 0, //sku单价

                          var spu_id: Long = 0L, //spu编号

                          var spu_name: String = null, //spu名称

                          var tm_id: Long = 0L, //品牌编号

                          var tm_name: String = null, //品牌名称

                          var category3_id: Long = 0L, //品类编号

                          var category3_name: String = null, //品类名称


                          @Builder.Default var display_ct: Long = 0L, //曝光数


                          @Builder.Default var click_ct: Long = 0L, //点击数


                          @Builder.Default var favor_ct: Long = 0L, //收藏数


                          @Builder.Default var cart_ct: Long = 0L, //添加购物车数


                          @Builder.Default var order_sku_num: Long = 0L, //下单商品个数


                          @Builder.Default
                          var order_amount: Double = 0, //下单商品金额

                          @Builder.Default var order_ct: Long = 0L, //订单数


                          @Builder.Default var payment_amount: Double = 0,

                          @Builder.Default var paid_order_ct: Long = 0L, //支付订单数


                          @Builder.Default var refund_order_ct: Long = 0L, //退款订单数


                          @Builder.Default var refund_amount: Double = 0.0,

                          @Builder.Default var comment_ct: Long = 0L, //评论订单数


                          @Builder.Default var good_comment_ct: Long = 0L, //好评订单数


                          @Builder.Default @TransientSink var orderIdSet: HashSet[Long] = new HashSet(), //用于统计订单数


                          @Builder.Default @TransientSink var paidOrderIdSet: HashSet[Long] = new HashSet(), //用于统计支付订单数


                          @Builder.Default @TransientSink var refundOrderIdSet: HashSet[Long] = new HashSet[Long](), //用于退款支付订单数


                          var ts: Long = 0L //统计时间戳

                        )
