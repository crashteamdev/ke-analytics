package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.domain.mongo.ProductSkuData
import dev.crashteam.keanalytics.domain.mongo.ProductSkuId

data class ProductHistory(
    val id: ProductSkuId,
    val skuChange: List<ProductSkuData>
)
