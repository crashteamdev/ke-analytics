package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal

data class ProductSplitSales(
    val skuId: Long,
    val name: String,
    val price: BigDecimal?,
    val priceDiscount: BigDecimal,
    val availableAmount: Long,
    val orderAmount: Long,
    val salesAmount: BigDecimal,
    val photoKey: String? = null,
)
