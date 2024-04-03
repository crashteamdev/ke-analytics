package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalyticsPair
import mu.KotlinLogging
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.sql.ResultSet

private val log = KotlinLogging.logger {}

class CategoryAnalyticsMapper : RowMapper<ChCategoryAnalyticsPair> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryAnalyticsPair {
        log.debug {
            "[CategoryAnalyticsMapper] Order per product: ${rs.getDouble("order_per_product")}"
        }
        return ChCategoryAnalyticsPair(
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            revenue = rs.getBigDecimal("revenue"),
            avgBill = rs.getBigDecimal("avg_bill"),
            sellerCount = rs.getLong("seller_count"),
            productCount = rs.getLong("product_count"),
            orderPerProduct = BigDecimal.valueOf(rs.getDouble("order_per_product")),
            orderPerSeller = BigDecimal.valueOf(rs.getDouble("order_per_seller")),
            revenuePerProduct = BigDecimal.valueOf(rs.getDouble("revenue_per_product")),
            prevOrderAmount = rs.getLong("prev_order_amount"),
            prevAvailableAmount = rs.getLong("prev_available_amount"),
            prevRevenue = rs.getBigDecimal("prev_revenue"),
            prevAvgBill = rs.getBigDecimal("prev_avg_bill"),
            prevSellerCount = rs.getLong("prev_seller_count"),
            prevProductCount = rs.getLong("prev_product_count"),
            prevOrderPerProduct = BigDecimal.valueOf(rs.getDouble("prev_order_per_product")),
            prevOrderPerSeller = BigDecimal.valueOf(rs.getDouble("prev_order_per_seller")),
            prevRevenuePerProduct = BigDecimal.valueOf(rs.getDouble("prev_revenue_per_product")),
        )
    }
}
