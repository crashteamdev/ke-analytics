package dev.crashteam.keanalytics.repository.clickhouse.model

import java.time.LocalDateTime

data class ChProductPosition(
    val fetchTime: LocalDateTime,
    val productId: Long,
    val skuId: Long,
    val categoryId: Long,
    val position: Long,
)
