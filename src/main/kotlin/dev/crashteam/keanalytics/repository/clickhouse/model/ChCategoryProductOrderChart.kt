package dev.crashteam.keanalytics.repository.clickhouse.model

data class ChCategoryProductOrderChart(
    val productId: String,
    val orderChart: List<Long>,
)
