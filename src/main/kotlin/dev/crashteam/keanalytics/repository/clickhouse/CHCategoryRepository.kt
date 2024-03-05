package dev.crashteam.keanalytics.repository.clickhouse

import dev.crashteam.keanalytics.repository.clickhouse.mapper.CategoryAnalyticsMapper
import dev.crashteam.keanalytics.repository.clickhouse.mapper.CategoryDailyAnalyticsMapper
import dev.crashteam.keanalytics.repository.clickhouse.mapper.CategoryHierarchyMapper
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalytics
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryDailyAnalytics
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryHierarchy
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.stereotype.Repository
import java.math.BigInteger
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.LocalDateTime

@Repository
class CHCategoryRepository(
    @Qualifier("clickHouseJdbcTemplate") private val jdbcTemplate: JdbcTemplate
) {

    private companion object {
        const val GET_CATEGORIES_ANALYTICS_SQL = """
            SELECT sum(order_amount)                 AS order_amount,
                   sum(available_amount)             AS available_amount,
                   sum(revenue)                      AS revenue,
                   if(order_amount > 0, quantile(median_price_with_sales), 0) AS median_price,
                   if(order_amount > 0, revenue / order_amount, 0)            AS avg_bill,
                   product_seller_count_tuple.1      AS seller_count,
                   product_seller_count_tuple.2      AS product_count,
                   order_amount / product_count      AS order_per_product,
                   order_amount / seller_count       AS order_per_seller,
                   if(order_amount > 0, revenue / product_count, 0)           AS revenue_per_product,
                   (SELECT uniq(seller_id), uniq(product_id)
                    FROM kazanex.ke_product_daily_sales
                    WHERE date BETWEEN ? AND ?
                          AND category_id IN
                              if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                                 0,
                                 dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                                 array(?))
                    ) AS product_seller_count_tuple
            FROM (
                     SELECT date,
                            category_id,
                            sumMerge(orders)                       AS order_amount,
                            sumMerge(available_amount)             as available_amount,
                            quantileMerge(median_price_with_sales) AS median_price_with_sales,
                            sumMerge(revenue)                      AS revenue,
                            uniqMerge(seller_count)                AS seller_count,
                            uniqMerge(product_count)               AS product_count
                     FROM kazanex.category_daily_stats p
                     WHERE date BETWEEN ? AND ?
                       AND category_id IN
                           if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                              0,
                              dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                              array(?))
                     GROUP BY category_id, date
                     %s
                     )
        """
        const val CATEGORY_DAILY_ANALYTICS_SQL = """
            SELECT date,
                   (if(sumMerge(orders) <= 0, 0, sumMerge(orders)))   AS order_amount,
                   (if(order_amount <= 0, 0, revenue / order_amount)) AS average_bill,
                   sumMerge(available_amount)                         as available_amount,
                   (if(sumMerge(revenue) <= 0, 0, sumMerge(revenue))) AS revenue
            FROM kazanex.category_daily_stats p
            WHERE date BETWEEN ? AND ?
              AND category_id IN
                  if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                     0,
                     dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                     array(?))
            GROUP BY date
        """
        const val GET_DESCENDANT_CATEGORIES_SQL = """
            SELECT dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, ?) AS categories
            FROM system.numbers
            LIMIT 1
        """
        const val GET_CATEGORY_HIERARCHY_SQL = """
            SELECT
                result_tuple.1 AS name,
                result_tuple.2 AS parent_id,
                dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 1) AS children_ids,
                dictGet('kazanex.categories_hierarchical_dictionary', ('title', 'parentCategoryId'), ?) AS result_tuple
            FROM system.numbers
            LIMIT 1
        """
    }

    fun getCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
        sort: SortBy? = null,
    ): ChCategoryAnalytics? {
        val sql = if (sort != null) {
            val sb = StringBuilder()
            sb.append("ORDER BY ")
            sort.sortFields.forEachIndexed { index, sortField ->
                if (index >= sort.sortFields.size - 1) {
                    sb.append("${sortField.fieldName} ${sortField.order.name}")
                } else {
                    sb.append("${sortField.fieldName} ${sortField.order.name},")
                }
            }
            String.format(GET_CATEGORIES_ANALYTICS_SQL, sb.toString())
        } else {
            String.format(GET_CATEGORIES_ANALYTICS_SQL, "")
        }
        return jdbcTemplate.queryForObject(
            sql,
            CategoryAnalyticsMapper(),
            fromTime, toTime, categoryId, categoryId, categoryId, fromTime, toTime, categoryId, categoryId, categoryId
        )
    }

    fun getCategoryDailyAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
    ): List<ChCategoryDailyAnalytics> {
        return jdbcTemplate.query(
            CATEGORY_DAILY_ANALYTICS_SQL,
            CategoryDailyAnalyticsMapper(),
            fromTime, toTime, categoryId, categoryId, categoryId
        )
    }

    fun getDescendantCategories(categoryId: Long, level: Short): List<Long>? {
        return jdbcTemplate.queryForObject(
            GET_DESCENDANT_CATEGORIES_SQL,
            { rs, _ -> (rs.getArray("categories").array as LongArray).toList() },
            categoryId, level
        )
    }

    fun getCategoryHierarchy(categoryId: Long): ChCategoryHierarchy? {
        return jdbcTemplate.queryForObject(
            GET_CATEGORY_HIERARCHY_SQL,
            CategoryHierarchyMapper(),
            categoryId, categoryId
        )
    }

    internal class CategoryAnalyticsStatementSetter(
        private val categoryId: Long,
        private val fromTime: LocalDateTime,
        private val toTime: LocalDateTime,
    ) : PreparedStatementSetter {
        override fun setValues(ps: PreparedStatement) {
            var l = 1
            ps.setObject(l++, fromTime)
            ps.setObject(l++, toTime)
            ps.setLong(l++, categoryId)
            ps.setLong(l++, categoryId)
            ps.setLong(l++, categoryId)
            ps.setObject(l++, fromTime)
            ps.setObject(l++, toTime)
            ps.setLong(l++, categoryId)
            ps.setLong(l++, categoryId)
            ps.setLong(l++, categoryId)
        }
    }
}
