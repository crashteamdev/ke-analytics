package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class ProductDailyAnalytics(
    val productId: String,
    val title: String,
    val category: ProductDailyAnalyticsCategory,
    val seller: ProductDailyAnalyticsSeller,
    val price: BigDecimal,
    val fullPrice: BigDecimal?,
    val reviewAmount: Long,
    val revenue: BigDecimal,
    val photoKey: String,
    val priceChart: List<BigDecimal>,
    val revenueChart: List<BigDecimal>,
    val orderChart: List<Long>,
    val availableChart: List<Long>,
    val firstDiscovered: LocalDateTime,
    val rating: BigDecimal,
)

data class ProductDailyAnalyticsCategory(
    val categoryId: Long,
    val categoryName: String,
)

data class ProductDailyAnalyticsSeller(
    val sellerLink: String,
    val sellerTitle: String,
)
