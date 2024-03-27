package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryProductsAnalytics
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class CategoryProductsAnalyticsMapper : RowMapper<ChCategoryProductsAnalytics> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryProductsAnalytics {
        return ChCategoryProductsAnalytics(
            productId = rs.getString("product_id"),
            title = rs.getString("title"),
            revenue = rs.getBigDecimal("revenue"),
            medianPrice = rs.getBigDecimal("median_price"),
            availableAmount = rs.getLong("available_amount"),
            orderAmount = rs.getLong("order_amount"),
            reviewsAmount = rs.getLong("reviews_amount"),
            photoKey = rs.getString("photo_key"),
            rating = rs.getBigDecimal("rating"),
            totalRowCount = rs.getInt("total_row_count"),
        )
    }
}
