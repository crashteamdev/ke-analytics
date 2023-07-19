package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDate

data class ChProductSalesHistory(
    val date: LocalDate,
    val productId: String,
    val skuId: String,
    val title: String,
    val orderAmount: Long,
    val reviewAmount: Long,
    val fullPrice: BigDecimal?,
    val purchasePrice: BigDecimal,
    val photoKey: String,
    val salesAmount: BigDecimal,
    val totalAvailableAmount: Long,
    val availableAmount: Long
)
