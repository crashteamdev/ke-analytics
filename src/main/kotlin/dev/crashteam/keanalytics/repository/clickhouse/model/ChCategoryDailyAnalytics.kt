package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDate

data class ChCategoryDailyAnalytics(
    val date: LocalDate,
    val revenue: BigDecimal,
    val averageBill: BigDecimal,
    val orderAmount: Long,
    val availableAmount: Long,
)
