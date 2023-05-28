package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal

data class MultipleProductHistorySalesV2(
    val id: MultipleProductHistorySalesId,
    val sellerTitle: String,
    val sellerLink: String,
    val sellerAccountId: Long?,
    val orderAmount: Long,
    val salesAmount: BigDecimal,
    val dailyOrder: BigDecimal,
)

data class MultipleProductHistorySalesId(
    val productId: Long
)
