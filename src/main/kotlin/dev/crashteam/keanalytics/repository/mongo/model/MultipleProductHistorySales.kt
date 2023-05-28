package dev.crashteam.keanalytics.repository.mongo.model

import dev.crashteam.keanalytics.domain.mongo.ProductSkuData
import dev.crashteam.keanalytics.domain.mongo.ProductSkuId

data class MultipleProductHistorySales(
    val id: ProductSkuId,
    val seller: MultipleProductHistorySalesSeller,
    val skuChange: List<ProductSkuData>
)

data class MultipleProductHistorySalesSeller(
    val title: String,
    val link: String,
    val accountId: Long?,
    val sellerAccountId: Long?
)

