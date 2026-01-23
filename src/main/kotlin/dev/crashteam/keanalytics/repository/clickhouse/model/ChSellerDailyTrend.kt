package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDate

data class ChSellerDailyTrend(
    val date: LocalDate,
    val orderAmount: Long,
    val availableAmount: Long,
    val revenue: BigDecimal,
    val avgBill: BigDecimal,
    val productCount: Long,
    val orderPerProduct: BigDecimal,
    val revenuePerProduct: BigDecimal,
)
