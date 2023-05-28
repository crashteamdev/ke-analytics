package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class ProductCategoryAggregate(
    val data: List<ProductHistorySkuAggregate>,
    val meta: ProductHistorySkuMetadata
)

data class ProductCategoryAggregateByParentCategoryAndSeller(
    val id: ProductCategoryAggregateId,
    val skuChange: List<ProductHistorySkuChange>
)

data class ProductSellerAggregate(
    val data: List<ProductHistorySkuAggregate>,
    val meta: ProductHistorySkuMetadata
)

data class ProductHistorySkuMetadata(
    val total: Long,
    val page: Int,
    val pages: Int
)

data class ProductHistorySkuAggregate(
    val id: ProductHistorySkuAggregateId,
    val parentCategory: String,
    val ancestorCategories: List<String>,
    val seller: ProductHistorySellerSku,
    val skuChange: List<ProductHistorySkuChange>,
)

data class ProductHistorySkuAggregateId(
    val productId: Long,
    val skuId: Long,
)

data class ProductCategoryAggregateId(
    val parentCategory: String,
    val link: String,
)

data class ProductHistorySkuChange(
    val date: LocalDateTime,
    val name: String,
    val totalOrderAmount: Long,
    val totalAvailableAmount: Long,
    val skuAvailableAmount: Long,
    val skuCharacteristic: List<ProductHistorySkuCharacteristic> = emptyList(),
    val price: BigDecimal,
    val fullPrice: BigDecimal?,
    val rating: Double?,
    val photoKey: String?,
    val reviewsAmount: Long
)

data class ProductHistorySellerSku(
    val title: String,
    val link: String,
    val accountId: Long?,
    val sellerAccountId: Long?
)

data class ProductHistorySkuCharacteristic(
    val type: String,
    val title: String,
    val value: String,
)
