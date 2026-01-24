package dev.crashteam.keanalytics.repository.clickhouse.model

data class ChTrendingProduct(
    val productId: String,
    val name: String,
    val photoKey: String?,
)
