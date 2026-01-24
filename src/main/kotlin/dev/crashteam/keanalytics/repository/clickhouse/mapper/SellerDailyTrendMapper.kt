package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChSellerDailyTrend
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.LocalDate

class SellerDailyTrendMapper : RowMapper<ChSellerDailyTrend> {
    override fun mapRow(
        rs: ResultSet,
        rowNum: Int,
    ): ChSellerDailyTrend =
        ChSellerDailyTrend(
            date = rs.getDate("date").toLocalDate(),
            orderAmount = rs.getLong("order_amount"),
            availableAmount = rs.getLong("available_amount"),
            revenue = rs.getBigDecimal("revenue"),
            avgBill = BigDecimal.valueOf(rs.getDouble("avg_bill")),
            productCount = rs.getLong("product_count"),
            orderPerProduct = BigDecimal.valueOf(rs.getDouble("order_per_product")),
            revenuePerProduct = BigDecimal.valueOf(rs.getDouble("revenue_per_product")),
        )
}
