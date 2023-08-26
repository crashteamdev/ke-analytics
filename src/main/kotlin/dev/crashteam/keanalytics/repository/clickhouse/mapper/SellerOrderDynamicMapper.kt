package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChSellerOrderDynamic
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class SellerOrderDynamicMapper : RowMapper<ChSellerOrderDynamic> {
    override fun mapRow(rs: ResultSet, rowNum: Int): ChSellerOrderDynamic {
        return ChSellerOrderDynamic(
            date = rs.getDate("date").toLocalDate(),
            orderAmount = rs.getLong("order_amount")
        )
    }
}
