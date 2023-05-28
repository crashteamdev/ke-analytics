package dev.crashteam.keanalytics.repository.mongo.pageable

import dev.crashteam.keanalytics.domain.mongo.ProductSkuData

data class ProductHistoryPageResult(
    val productId: Long,
    val skuId: Long,
    val result: PageResult<ProductSkuData>?
)

data class ProductHistoryResultAggr(
    val skuChange: List<ProductSkuData>,
    val count: Int
)
