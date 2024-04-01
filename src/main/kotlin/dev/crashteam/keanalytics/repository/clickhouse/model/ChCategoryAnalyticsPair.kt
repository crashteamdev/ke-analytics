package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal

data class ChCategoryAnalyticsPair(
    val orderAmount: Long,
    val availableAmount: Long,
    val revenue: BigDecimal,
    val avgBill: BigDecimal,
    val sellerCount: Long,
    val productCount: Long,
    val orderPerProduct: BigDecimal,
    val orderPerSeller: BigDecimal,
    val revenuePerProduct: BigDecimal,
    val prevOrderAmount: Long,
    val prevAvailableAmount: Long,
    val prevRevenue: BigDecimal,
    val prevAvgBill: BigDecimal,
    val prevSellerCount: Long,
    val prevProductCount: Long,
    val prevOrderPerProduct: BigDecimal,
    val prevOrderPerSeller: BigDecimal,
    val prevRevenuePerProduct: BigDecimal,
)
