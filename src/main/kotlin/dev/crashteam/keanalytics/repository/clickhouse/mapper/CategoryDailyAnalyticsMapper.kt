package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryDailyAnalytics
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class CategoryDailyAnalyticsMapper : RowMapper<ChCategoryDailyAnalytics> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryDailyAnalytics {
        return ChCategoryDailyAnalytics(
            date = rs.getDate("date").toLocalDate(),
            revenue = rs.getBigDecimal("revenue"),
            averageBill = rs.getBigDecimal("average_bill"),
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
        )
    }
}
