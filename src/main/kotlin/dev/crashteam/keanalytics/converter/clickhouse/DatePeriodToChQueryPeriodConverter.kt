package dev.crashteam.keanalytics.converter.clickhouse

import dev.crashteam.keanalytics.service.model.QueryPeriod
import dev.crashteam.mp.base.DatePeriod
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
class DatePeriodToChQueryPeriodConverter : Converter<DatePeriod, QueryPeriod> {

    override fun convert(source: DatePeriod): QueryPeriod {
        return when (source) {
            DatePeriod.DATE_PERIOD_UNSPECIFIED, DatePeriod.UNRECOGNIZED -> QueryPeriod.MONTH
            DatePeriod.DATE_PERIOD_WEEK -> QueryPeriod.WEEK
            DatePeriod.DATE_PERIOD_TWO_WEEK -> QueryPeriod.TWO_WEEK
            DatePeriod.DATE_PERIOD_MONTH -> QueryPeriod.MONTH
            DatePeriod.DATE_PERIOD_TWO_MONTH -> QueryPeriod.TWO_MONTH
        }
    }
}
