package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal

data class ChProductSalesReport(
    val productId: String,
    val sellerTitle: String,
    val sellerId: Long,
    val latestCategoryId: Long,
    val orderGraph: List<Long>,
    val availableAmountGraph: List<Long>,
    val priceGraph: List<BigDecimal>,
    val availableAmounts: Long,
    val purchasePrice: BigDecimal,
    val sales: BigDecimal,
    val categoryName: String,
    val name: String,
    val total: Long,
)
