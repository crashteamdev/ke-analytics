package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalyticsPair
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class CategoryAnalyticsMapper : RowMapper<ChCategoryAnalyticsPair> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryAnalyticsPair {
        return ChCategoryAnalyticsPair(
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            revenue = rs.getBigDecimal("revenue"),
            avgBill = rs.getBigDecimal("avg_bill"),
            sellerCount = rs.getLong("seller_count"),
            productCount = rs.getLong("product_count"),
            orderPerProduct = rs.getString("order_per_product").toBigDecimal(),
            orderPerSeller = rs.getString("order_per_seller").toBigDecimal(),
            revenuePerProduct = rs.getString("revenue_per_product").toBigDecimal(),
            prevOrderAmount = rs.getLong("prev_order_amount"),
            prevAvailableAmount = rs.getLong("prev_available_amount"),
            prevRevenue = rs.getBigDecimal("prev_revenue"),
            prevAvgBill = rs.getBigDecimal("prev_avg_bill"),
            prevSellerCount = rs.getLong("prev_seller_count"),
            prevProductCount = rs.getLong("prev_product_count"),
            prevOrderPerProduct = rs.getString("prev_order_per_product").toBigDecimal(),
            prevOrderPerSeller = rs.getString("prev_order_per_seller").toBigDecimal(),
            prevRevenuePerProduct = rs.getString("prev_revenue_per_product").toBigDecimal(),
        )
    }
}
