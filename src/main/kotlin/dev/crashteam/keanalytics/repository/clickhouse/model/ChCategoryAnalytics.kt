package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal

data class ChCategoryAnalytics(
    val orderAmount: Long,
    val availableAmount: Long,
    val revenue: BigDecimal,
    val medianPrice: BigDecimal,
    val avgBill: BigDecimal,
    val sellerCount: Long,
    val productCount: Long,
    val orderPerProduct: BigDecimal,
    val orderPerSeller: BigDecimal,
    val revenuePerProduct: BigDecimal,
)
