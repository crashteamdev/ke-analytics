package dev.crashteam.keanalytics.controller.model

import java.math.BigDecimal

data class ProductTotalSalesView(
    val productId: Long,
    val salesAmount: BigDecimal,
    val orderAmount: Long,
    val dailyOrder: BigDecimal,
    val seller: ProductTotalSalesSeller
)

data class ProductTotalSalesSeller(
    val title: String,
    val link: String,
    val accountId: Long?
)

data class ProductSkuTotalSalesView(
    val productId: Long,
    val skuId: Long,
    val salesAmount: BigDecimal,
    val orderAmount: Long,
    val seller: ProductTotalSalesSeller
)
