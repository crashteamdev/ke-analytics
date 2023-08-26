package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.repository.clickhouse.model.ChSellerOrderDynamic
import java.math.BigDecimal

data class SellerOverallInfo(
    val averagePrice: BigDecimal,
    val revenue: BigDecimal,
    val orderCount: Long,
    val productCount: Long,
    val productCountWithSales: Long,
    val salesDynamic: List<ChSellerOrderDynamic>,
)
