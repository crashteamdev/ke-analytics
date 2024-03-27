package dev.crashteam.keanalytics.repository.clickhouse.model

data class SortBy(
    val sortFields: List<SortField>,
)

data class SortField(
    val fieldName: String,
    val order: SortOrder,
)

enum class SortOrder {
    ASC, DESC
}
