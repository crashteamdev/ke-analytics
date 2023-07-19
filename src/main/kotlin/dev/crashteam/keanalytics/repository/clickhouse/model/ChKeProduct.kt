package dev.crashteam.keanalytics.repository.clickhouse.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class ChKeProduct(
    val fetchTime: LocalDateTime,
    val productId: Long,
    val skuId: Long,
    val title: String,
    val categoryPaths: List<Long>,
    val rating: BigDecimal,
    val reviewsAmount: Int,
    val totalOrdersAmount: Long,
    val totalAvailableAmount: Long,
    val availableAmount: Long,
    val fullPrice: Long?,
    val purchasePrice: Long,
    val attributes: List<String>,
    val tags: List<String>,
    val photoKey: String?,
    val characteristics: List<ChKazanExpressCharacteristic>,
    val sellerId: Long,
    val sellerAccountId: Long,
    val sellerTitle: String,
    val sellerLink: String,
    val sellerRegistrationDate: Long,
    val sellerRating: BigDecimal,
    val sellerReviewsCount: Int,
    val sellerOrders: Long,
    val sellerContacts: Map<String, String>,
    val isEco: Boolean,
    val adultCategory: Boolean,
)

data class ChKazanExpressCharacteristic(
    val type: String,
    val title: String
)
