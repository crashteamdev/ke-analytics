package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.repository.clickhouse.model.ChTrendingProduct

data class TrendingProductsCacheableWrapper(
    val trendingProducts: List<ChTrendingProduct>?,
)
