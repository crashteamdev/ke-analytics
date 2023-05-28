package dev.crashteam.keanalytics.controller.model

import java.math.BigDecimal

class ProductTotalOrdersView(
    val totalOrderAmount: Long,
    val earnings: BigDecimal,
    val quantity: Long,
    val dailyOrder: BigDecimal,
    val seller: ProductTotalOrderSellerView,
)

class ProductTotalOrderSellerView(
    val title: String,
    val link: String,
    val accountId: Long?
)
