package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryProductOrderChart
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class CategoryProductOrderChartRowMapper : RowMapper<ChCategoryProductOrderChart> {
    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryProductOrderChart? {
        return ChCategoryProductOrderChart(
            rs.getString("product_id"),
            (rs.getArray("order_amount_chart").array as LongArray).toList()
        )
    }
}
