package dev.crashteam.keanalytics.repository.clickhouse.model

import java.time.LocalDate

data class ChProductPositionHistory(
    val date: LocalDate,
    val categoryId: String,
    val productId: String,
    val skuId: String,
    val position: Long,
)
