package dev.crashteam.keanalytics.repository.clickhouse.model

import java.time.LocalDate

data class FilterBy(
    val sqlFilterFields: List<SqlFilterField>,
)

sealed interface SqlFilterField {
    val fieldName: String
    fun sqlPredicate(): String
    val type: SqlFilterFieldType
}

enum class SqlFilterFieldType {
    String,
    Text,
    Numeric,
    Date
}

class EqualsSqlFilter(
    override val fieldName: String,
    private val value: String
) : SqlFilterField {
    override fun sqlPredicate(): String = "$fieldName = $value"
    override val type: SqlFilterFieldType = SqlFilterFieldType.String
}

class LikeSqlFilter(
    override val fieldName: String,
    private val value: String
) : SqlFilterField {
    override fun sqlPredicate(): String = "$fieldName ILIKE '%$value%'"
    override val type: SqlFilterFieldType = SqlFilterFieldType.Text
}

class BetweenSqlFilter<T : Comparable<T>>(
    override val fieldName: String,
    private val leftValue: T,
    private val rightValue: T
) : SqlFilterField {
    override fun sqlPredicate(): String = "$fieldName BETWEEN $leftValue AND $rightValue"
    override val type: SqlFilterFieldType = when (leftValue) {
        is String -> SqlFilterFieldType.String
        is LocalDate -> SqlFilterFieldType.Date
        is Long, is Int, is Short, is Byte -> SqlFilterFieldType.Numeric
        else -> throw IllegalArgumentException("Unsupported filter type")
    }
}
