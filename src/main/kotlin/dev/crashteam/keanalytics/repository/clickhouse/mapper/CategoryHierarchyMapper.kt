package dev.crashteam.keanalytics.repository.clickhouse.mapper

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryHierarchy
import org.springframework.jdbc.core.RowMapper
import java.math.BigInteger
import java.sql.ResultSet

class CategoryHierarchyMapper : RowMapper<ChCategoryHierarchy> {

    override fun mapRow(rs: ResultSet, rowNum: Int): ChCategoryHierarchy {
        return ChCategoryHierarchy(
            name = rs.getString("name"),
            parentId = rs.getLong("parent_id"),
            childrenIds = (rs.getArray("children_ids").array as Array<BigInteger>).map { it.toLong() }.toList(),
        )
    }
}
