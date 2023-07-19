package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductsSales
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import java.sql.ResultSet

@Component
class ProductsSalesMapper : RowMapper<ChProductsSales> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChProductsSales? {
        return ChProductsSales(
            productId = rs.getString("product_id"),
            title = rs.getString("title"),
            orderAmount = rs.getLong("order_amount"),
            dailyOrderAmount = rs.getBigDecimal("daily_order_amount"),
            salesAmount = rs.getBigDecimal("sales_amount"),
            sellerTitle = rs.getString("seller_title"),
            sellerLink = rs.getString("seller_link"),
            sellerAccountId = rs.getLong("seller_account_id")
        )
    }
}
