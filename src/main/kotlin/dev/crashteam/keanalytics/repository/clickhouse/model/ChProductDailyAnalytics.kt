package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class ChProductDailyAnalytics(
    val productId: String,
    val title: String,
    val categoryId: Long,
    val sellerLink: String,
    val sellerTitle: String,
    val price: BigDecimal,
    val fullPrice: BigDecimal,
    val reviewAmount: Long,
    val revenue: BigDecimal,
    val photoKey: String,
    val priceChart: List<BigDecimal>,
    val revenueChart: List<BigDecimal>,
    val orderChart: List<Long>,
    val availableChart: List<Long>,
    val firstDiscovered: LocalDateTime,
)
