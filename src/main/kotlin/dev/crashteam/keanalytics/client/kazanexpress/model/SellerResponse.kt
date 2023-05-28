package dev.crashteam.keanalytics.client.kazanexpress.model

import java.math.BigDecimal

data class SellerResponse(
    val payload: SellerPayload?,
    val error: String?,
)

data class SellerPayload(
    val products: List<SellerProduct>,
    val adultContent: Boolean,
    val totalProducts: Int
)

data class SellerProduct(
    val productId: Long,
    val categoryId: Long,
    val title: String,
    val sellPrice: BigDecimal,
    val fullPrice: BigDecimal?,
    val compressedImage: String,
    val image: String,
    val rating: BigDecimal,
    val ordersQuantity: Long,
    val rOrdersQuantity: Long,
)
