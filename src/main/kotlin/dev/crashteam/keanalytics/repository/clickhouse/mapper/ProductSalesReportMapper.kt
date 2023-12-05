package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesReport
import org.springframework.jdbc.core.RowMapper
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.ResultSet

class ProductSalesReportMapper : RowMapper<ChProductSalesReport> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChProductSalesReport {
        return ChProductSalesReport(
            productId = rs.getString("product_id"),
            sellerTitle = rs.getString("seller_title"),
            latestCategoryId = rs.getLong("latest_category_id"),
            sellerId = rs.getLong("seller_id"),
            orderGraph = (rs.getArray("order_graph").array as LongArray).toList(),
            priceGraph = (rs.getArray("price_graph").array as DoubleArray).map { BigDecimal.valueOf(it) }.toList(),
            availableAmountGraph = ((rs.getArray("available_amount_graph").array) as Array<BigInteger>).map { it.toLong() }.toList(),
            availableAmounts = rs.getLong("available_amounts"),
            purchasePrice = rs.getBigDecimal("purchase_price"),
            sales = rs.getBigDecimal("sales"),
            categoryName = rs.getString("category_name"),
            name = rs.getString("name"),
            total = rs.getLong("total"),
        )
    }
}
