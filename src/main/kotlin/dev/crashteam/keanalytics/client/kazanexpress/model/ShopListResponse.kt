package dev.crashteam.keanalytics.client.kazanexpress.model

import java.math.BigDecimal

data class ShopListResponse(
    val payload: List<ShopItem>?
)

data class ShopItem(
    val productId: String,
    val title: String,
    val sellPrice: BigDecimal,
    val fullPrice: BigDecimal,
    val compressedImage: String,
    val image: String,
    val rating: BigDecimal,
    val ordersQuantity: Long,
    val rOrdersQuantity: Long,
)
