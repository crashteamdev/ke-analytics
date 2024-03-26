package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductDailyAnalytics
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesReport
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.ResultSet

class ProductDailyAnalyticsMapper : RowMapper<ChProductDailyAnalytics> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChProductDailyAnalytics {
        return ChProductDailyAnalytics(
            productId = rs.getString("product_id"),
            title = rs.getString("title"),
            categoryId = rs.getLong("category_id"),
            sellerLink = rs.getString("seller_link"),
            sellerTitle = rs.getString("seller_title"),
            price = rs.getBigDecimal("price"),
            fullPrice = rs.getBigDecimal("full_price"),
            reviewAmount = rs.getLong("review_amount"),
            revenue = rs.getBigDecimal("revenue"),
            photoKey = rs.getString("photo_key"),
            priceChart = (rs.getArray("price_chart").array as DoubleArray).map { BigDecimal.valueOf(it) }.toList(),
            revenueChart = (rs.getArray("revenue_chart").array as DoubleArray).map { BigDecimal.valueOf(it) }.toList(),
            orderChart = (rs.getArray("order_chart").array as LongArray).toList(),
            availableChart = (rs.getArray("available_chart").array as LongArray).toList(),
            firstDiscovered = rs.getTimestamp("first_discovered").toLocalDateTime(),
            rating = rs.getBigDecimal("rating")
        )
    }
}
