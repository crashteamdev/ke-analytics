package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.*

data class ProductHistorySkuAggregateV2(
    val data: List<ProductHistorySkuData>,
    val total: Long,
)

data class ProductHistorySkuData(
    val id: ProductHistorySkuDataId,
    val productId: Long,
    val skuId: Long,
    val name: String,
    val reviewsAmount: Long,
    val orderAmount: Long,
    val totalAvailableAmount: Long,
    val availableAmount: Long,
    val fullPrice: BigDecimal,
    val price: BigDecimal,
    val photoKey: String,
    val salesAmount: BigDecimal,
)

data class ProductHistorySkuDataId(
    val day: Date
)
