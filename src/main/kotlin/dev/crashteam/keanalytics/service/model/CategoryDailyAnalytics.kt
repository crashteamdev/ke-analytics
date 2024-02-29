package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal
import java.time.LocalDate

data class CategoryDailyAnalytics(
    val date: LocalDate,
    val revenue: BigDecimal,
    val averageBill: BigDecimal,
    val salesCount: Long,
    val availableCount: Long,
)
