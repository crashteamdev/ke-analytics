package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal

data class ChProductsSales(
    val productId: String,
    val title: String,
    val orderAmount: Long,
    val dailyOrderAmount: BigDecimal,
    val salesAmount: BigDecimal,
    val sellerTitle: String,
    val sellerLink: String,
    val sellerAccountId: Long
)
