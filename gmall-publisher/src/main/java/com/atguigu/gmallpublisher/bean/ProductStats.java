package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Desc: 商品交易额统计实体类
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {
    String stt;
    String edt;
    Long sku_id;
    String sku_name;
    BigDecimal sku_price;
    Long spu_id;
    String spu_name;
    Long tm_id ;
    String tm_name;
    Long category3_id ;
    String category3_name ;
    @Builder.Default
    Long  display_ct=0L;
    @Builder.Default
    Long  click_ct=0L;
    @Builder.Default
    Long  cart_ct=0L;
    @Builder.Default
    Long  order_sku_num=0L;
    @Builder.Default
    BigDecimal order_amount=BigDecimal.ZERO;
    @Builder.Default
    Long  order_ct=0L;
    @Builder.Default
    BigDecimal  payment_amount=BigDecimal.ZERO;
    @Builder.Default
    Long  refund_ct=0L;
    @Builder.Default
    BigDecimal  refund_amount=BigDecimal.ZERO;
    @Builder.Default
    Long  comment_ct=0L;
    @Builder.Default
    Long  good_comment_ct=0L ;
    Long ts;
}
