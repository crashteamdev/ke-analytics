package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.extensions.getNullableBigDecimal
import dev.crashteam.keanalytics.extensions.getNullableBigDecimal
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesHistory
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import java.sql.ResultSet

@Component
class ProductSalesHistoryMapper : RowMapper<ChProductSalesHistory> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChProductSalesHistory? {
        return ChProductSalesHistory(
            date = rs.getDate("date").toLocalDate(),
            productId = rs.getString("product_id"),
            skuId = rs.getString("sku_id"),
            title = rs.getString("title"),
            orderAmount = rs.getLong("order_amount"),
            reviewAmount = rs.getLong("review_amount"),
            fullPrice = rs.getNullableBigDecimal("full_price"),
            purchasePrice = rs.getBigDecimal("purchase_price"),
            photoKey = rs.getString("photo_key"),
            salesAmount = rs.getBigDecimal("sales_amount"),
            totalAvailableAmount = rs.getLong("total_available_amount"),
            availableAmount = rs.getLong("available_amount")
        )
    }
}
