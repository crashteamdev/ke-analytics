package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDate

data class ChTrendingProductAnalytics(
    val productId: String,
    val title: String,
    val revenue: BigDecimal,
    val medianPrice: BigDecimal,
    val orderAmount: Long,
    val availableAmount: Long,
    val reviewsAmount: Long,
    val photoKey: String?,
    val rating: BigDecimal,
    val sellerLink: String,
    val sellerTitle: String,
    val sellerAccountId: String,
    val firstSeenDate: LocalDate,
    val totalRowCount: Int,
)
