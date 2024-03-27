package dev.crashteam.keanalytics.converter.clickhouse

import dev.crashteam.keanalytics.extensions.toLocalDate
import dev.crashteam.keanalytics.repository.clickhouse.model.BetweenSqlFilter
import dev.crashteam.keanalytics.repository.clickhouse.model.EqualsSqlFilter
import dev.crashteam.keanalytics.repository.clickhouse.model.LikeSqlFilter
import dev.crashteam.keanalytics.repository.clickhouse.model.SqlFilterField
import dev.crashteam.mp.base.Filter
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
class ProtoFilterToChSqlConverter : Converter<Filter, SqlFilterField> {

    override fun convert(source: Filter): SqlFilterField? {
        return if (source.condition.hasEqualsTextCondition()) {
            LikeSqlFilter(source.fieldName, source.condition.equalsTextCondition.text)
        } else if (source.condition.hasEqualsValueCondition()) {
            EqualsSqlFilter(source.fieldName, source.condition.equalsTextCondition.text)
        } else if (source.condition.hasFilterBetweenCondition()) {
            BetweenSqlFilter(
                source.fieldName,
                source.condition.filterBetweenCondition.left,
                source.condition.filterBetweenCondition.right,
            )
        } else if (source.condition.hasFilterBetweenDateCondition()) {
            BetweenSqlFilter(
                source.fieldName,
                source.condition.filterBetweenDateCondition.dateRangeCondition.fromDate.toLocalDate(),
                source.condition.filterBetweenDateCondition.dateRangeCondition.toDate.toLocalDate(),
            )
        } else throw IllegalArgumentException("Unknown filter type: $source")
    }
}
