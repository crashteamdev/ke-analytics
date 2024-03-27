package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalytics
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class CategoryAnalyticsMapper : RowMapper<ChCategoryAnalytics> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryAnalytics {
        return ChCategoryAnalytics(
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            revenue = rs.getBigDecimal("revenue"),
            medianPrice = rs.getBigDecimal("median_price"),
            avgBill = rs.getBigDecimal("avg_bill"),
            sellerCount = rs.getLong("seller_count"),
            productCount = rs.getLong("product_count"),
            orderPerProduct = rs.getBigDecimal("order_per_product"),
            orderPerSeller = rs.getBigDecimal("order_per_seller"),
            revenuePerProduct = rs.getBigDecimal("revenue_per_product"),
        )
    }
}
