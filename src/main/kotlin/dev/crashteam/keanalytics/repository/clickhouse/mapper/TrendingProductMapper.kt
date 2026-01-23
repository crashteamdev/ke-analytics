package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChTrendingProduct
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class TrendingProductMapper : RowMapper<ChTrendingProduct> {
    override fun mapRow(
        rs: ResultSet,
        rowNum: Int,
    ): ChTrendingProduct =
        ChTrendingProduct(
            productId = rs.getString("product_id"),
            name = rs.getString("title"),
            photoKey = rs.getString("photo_key"),
        )
}
