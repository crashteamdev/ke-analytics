package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductAdditionalInfo
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

class ProductAdditionalInfoMapper : RowMapper<ChProductAdditionalInfo> {
    override fun mapRow(rs: ResultSet, rowNum: Int): ChProductAdditionalInfo {
        return ChProductAdditionalInfo(
            firstDiscovered = rs.getTimestamp("first_discovered").toLocalDateTime()
        )
    }
}
