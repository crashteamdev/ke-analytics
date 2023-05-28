package dev.crashteam.keanalytics.domain.mongo

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.math.BigDecimal
import java.time.Instant

@Document("product_change_history_v2")
data class ProductChangeHistoryDocument(
    @Id
    val id: ProductSkuId,
    val parentCategory: String,
    val ancestorCategories: List<String>,
    val seller: SellerData,
    val skuChange: List<ProductSkuData>,
)

data class ProductSkuId(
    val productId: Long,
    val skuId: Long,
)

data class ProductSkuData(
    val date: Instant,
    val rating: BigDecimal?,
    val reviewsAmount: Long,
    val totalOrderAmount: Long,
    val totalAvailableAmount: Long,
    val skuAvailableAmount: Long,
    val skuCharacteristic: List<ProductSkuCharacteristic>?,
    val name: String,
    val price: BigDecimal,
    val fullPrice: BigDecimal?,
    val photoKey: String?,
)

data class ProductSkuCharacteristic(
    val type: String,
    val title: String,
    val value: String,
)

data class SellerData(
    val title: String,
    val link: String,
    val accountId: Long?,
    val sellerAccountId: Long?
)
