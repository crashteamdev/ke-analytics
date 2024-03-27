package dev.crashteam.keanalytics.repository.clickhouse.model

data class ChCategoryAnalyticsPair(
    val chCategoryAnalytics: ChCategoryAnalytics,
    val prevChCategoryAnalytics: ChCategoryAnalytics,
)
