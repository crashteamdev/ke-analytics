package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryDailyTrend
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.LocalDate

class CategoryDailyTrendMapper : RowMapper<ChCategoryDailyTrend> {
    override fun mapRow(
        rs: ResultSet,
        rowNum: Int,
    ): ChCategoryDailyTrend =
        ChCategoryDailyTrend(
            date = rs.getDate("date").toLocalDate(),
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            revenue = rs.getBigDecimal("revenue"),
            avgBill = BigDecimal.valueOf(rs.getDouble("avg_bill")),
            sellerCount = rs.getLong("seller_count"),
            productCount = rs.getLong("product_count"),
            orderPerProduct = BigDecimal.valueOf(rs.getDouble("order_per_product")),
            orderPerSeller = BigDecimal.valueOf(rs.getDouble("order_per_seller")),
            revenuePerProduct = BigDecimal.valueOf(rs.getDouble("revenue_per_product")),
        )
}
