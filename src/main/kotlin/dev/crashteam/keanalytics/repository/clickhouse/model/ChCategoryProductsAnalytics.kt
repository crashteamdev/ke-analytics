package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal

data class ChCategoryProductsAnalytics(
    val productId: String,
    val title: String,
    val revenue: BigDecimal,
    val medianPrice: BigDecimal,
    val orderAmount: Long,
    val availableAmount: Long,
    val reviewsAmount: Long,
    val photoKey: String,
    val rating: BigDecimal,
    val totalRowCount: Int,
)
