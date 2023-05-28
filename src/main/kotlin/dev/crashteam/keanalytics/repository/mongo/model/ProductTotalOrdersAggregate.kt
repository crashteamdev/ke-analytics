package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal

data class ProductTotalOrdersAggregate(
    val price: BigDecimal,
    val totalOrderAmount: Long,
    val quantity: Long,
    val earnings: BigDecimal,
    val seller: ProductTotalOrdersSeller,
    val dailyOrder: BigDecimal
)

data class ProductTotalOrdersSeller(
    val title: String,
    val link: String,
    val accountId: Long?,
    val sellerAccountId: Long?
)
