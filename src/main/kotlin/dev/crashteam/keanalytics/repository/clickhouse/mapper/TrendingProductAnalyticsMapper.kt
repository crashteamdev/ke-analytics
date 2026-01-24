package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChTrendingProductAnalytics
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.LocalDate

class TrendingProductAnalyticsMapper : RowMapper<ChTrendingProductAnalytics> {
    override fun mapRow(
        rs: ResultSet,
        rowNum: Int,
    ): ChTrendingProductAnalytics =
        ChTrendingProductAnalytics(
            productId = rs.getString("product_id"),
            title = rs.getString("title"),
            revenue = rs.getBigDecimal("revenue"),
            medianPrice = rs.getBigDecimal("price"),
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            reviewsAmount = rs.getLong("reviews_amount"),
            photoKey = rs.getString("photo_key"),
            rating = BigDecimal.valueOf(rs.getDouble("rating")),
            sellerLink = rs.getString("seller_link"),
            sellerTitle = rs.getString("seller_title"),
            sellerAccountId = rs.getString("seller_account_id"),
            firstSeenDate = rs.getDate("first_seen_date").toLocalDate(),
            totalRowCount = rs.getInt("total_row_count"),
        )
}
