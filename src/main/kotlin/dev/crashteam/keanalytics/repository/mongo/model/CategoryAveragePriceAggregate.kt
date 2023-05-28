package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal

data class CategoryAveragePriceAggregate(
    val productCount: Long,
    val averagePrice: BigDecimal
)
