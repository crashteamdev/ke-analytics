package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.repository.mongo.model.ProductHistorySkuChange

data class ProductSkuDayLink(
    val sku: ProductHistorySkuChange,
    val nextDaySku: ProductHistorySkuChange,
)
