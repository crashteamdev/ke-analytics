package dev.crashteam.keanalytics.client.kazanexpress.model

import java.math.BigDecimal

data class CategoryResponse(
    val payload: CategoryPayload?,
    val error: String?
)

data class CategoryPayload(
    val parents: List<Category>,
    val category: Category,
    val products: List<Product>
)

data class Category(
    val id: Long,
    val productAmount: Long,
    val adult: Boolean,
    val eco: Boolean,
    val title: String,
    val path: List<Long>?,
    val children: List<Category>,
)

data class Product(
    val productId: Long,
    val title: String,
    val sellPrice: BigDecimal,
    val fullPrice: BigDecimal,
    val compressedImage: String,
    val image: String,
    val rating: BigDecimal,
    val ordersQuantity: Long,
    val rOrdersQuantity: Long,
)
