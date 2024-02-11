package dev.crashteam.keanalytics.repository.clickhouse

import dev.crashteam.keanalytics.repository.clickhouse.mapper.ProductPositionHistoryMapper
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPosition
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPositionHistory
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesHistory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

@Repository
class CHProductPositionRepository(
    @Qualifier("clickHouseJdbcTemplate") private val jdbcTemplate: JdbcTemplate
) {
    private companion object {
        const val GET_PRODUCT_POSITION_HISTORY = """
            SELECT date,
                   anyLast(position) AS position
            FROM kazanex.product_position
            WHERE timestamp BETWEEN ? AND ?
                AND category_id = ?
                AND product_id = ?
                AND sku_id = ?
            GROUP BY toDate(timestamp) AS date
            ORDER BY date WITH FILL FROM toDate(?) TO  toDate(?);
        """
    }

    fun saveProductsPosition(productPositionFetchList: List<ChProductPosition>) {
        jdbcTemplate.batchUpdate(
            "INSERT INTO kazanex.product_position " +
                    "(timestamp, product_id, sku_id, category_id, position)" +
                    " VALUES (?, ?, ?, ?, ?)",
            ProductPositionBatchPreparedStatementSetter(productPositionFetchList)
        )
    }

    fun getProductPositionHistory(
        categoryId: String,
        productId: String,
        skuId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): MutableList<ChProductPositionHistory> {
        return jdbcTemplate.query(
            GET_PRODUCT_POSITION_HISTORY,
            ProductPositionHistoryStatementSetter(categoryId, productId, skuId, fromTime, toTime),
            ProductPositionHistoryMapper()
        )
    }

    internal class ProductPositionHistoryStatementSetter(
        private val categoryId: String,
        private val productId: String,
        private val skuId: String,
        private val fromTime: LocalDateTime,
        private val toTime: LocalDateTime,
    ) : PreparedStatementSetter {
        override fun setValues(ps: PreparedStatement) {
            var l = 1
            ps.setObject(l++, fromTime.toLocalDate())
            ps.setObject(l++, toTime.toLocalDate())
            ps.setString(l++, categoryId)
            ps.setString(l++, productId)
            ps.setString(l++, skuId)
            ps.setObject(l++, fromTime.toLocalDate())
            ps.setObject(l++, toTime.toLocalDate())
        }
    }

    internal class ProductPositionBatchPreparedStatementSetter(
        private val products: List<ChProductPosition>
    ) : BatchPreparedStatementSetter {

        override fun setValues(ps: PreparedStatement, i: Int) {
            val product: ChProductPosition = products[i]
            var l = 1
            ps.setObject(
                l++, Instant.ofEpochSecond(
                    product.fetchTime.toEpochSecond(ZoneOffset.UTC)
                ).atZone(ZoneId.of("UTC")).toLocalDateTime()
            )
            ps.setLong(l++, product.productId)
            ps.setLong(l++, product.skuId)
            ps.setLong(l++, product.categoryId)
            ps.setLong(l++, product.position)
        }

        override fun getBatchSize(): Int {
            return products.size
        }
    }

}
