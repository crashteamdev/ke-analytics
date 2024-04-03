package dev.crashteam.keanalytics.repository.clickhouse

import dev.crashteam.keanalytics.repository.clickhouse.mapper.*
import dev.crashteam.keanalytics.repository.clickhouse.model.*
import dev.crashteam.keanalytics.service.model.QueryPeriod
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@Repository
class CHCategoryRepository(
    @Qualifier("clickHouseJdbcTemplate") private val jdbcTemplate: JdbcTemplate
) {

    private companion object {
        const val GET_CATEGORIES_ANALYTICS_WITH_PREV_SQL = """
            SELECT sum(order_amount)                                               AS order_amount,
                   sum(available_amount)                                           AS available_amount,
                   sum(revenue) / 100                                              AS revenue,
                   if(order_amount > 0, revenue / order_amount, 0)                 AS avg_bill,
                   product_seller_count_tuple.1                                    AS seller_count,
                   product_seller_count_tuple.2                                    AS product_count,
                   order_amount / product_count                                    AS order_per_product,
                   order_amount / seller_count                                     AS order_per_seller,
                   if(order_amount > 0, revenue / product_count, 0)                AS revenue_per_product,
                   sum(prev_order_amount)                                          AS prev_order_amount,
                   sum(prev_available_amount)                                      AS prev_available_amount,
                   sum(prev_revenue) / 100                                         AS prev_revenue,
                   if(prev_order_amount > 0, prev_revenue / prev_order_amount, 0)  AS prev_avg_bill,
                   prev_product_seller_count_tuple.1                               AS prev_seller_count,
                   prev_product_seller_count_tuple.2                               AS prev_product_count,
                   prev_order_amount / prev_product_count                          AS prev_order_per_product,
                   prev_order_amount / prev_seller_count                           AS prev_order_per_seller,
                   if(prev_order_amount > 0, prev_revenue / prev_product_count, 0) AS prev_revenue_per_product,
                   (SELECT uniq(seller_id), uniq(product_id)
                    FROM kazanex.ke_product_daily_sales
                    WHERE date BETWEEN ? AND ?
                      AND category_id IN
                          if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                             0,
                             dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                             array(?)))                                        AS product_seller_count_tuple,
                   (SELECT uniq(seller_id), uniq(product_id)
                    FROM kazanex.ke_product_daily_sales
                    WHERE date BETWEEN ? AND ?
                      AND category_id IN
                          if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                             0,
                             dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                             array(?)))                                        AS prev_product_seller_count_tuple
            FROM %s
            WHERE category_id IN
                  if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                     0,
                     dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                     array(?)) AND (date = ?)
        """
        const val GET_CATEGORIES_ANALYTICS_SQL = """
            SELECT sum(final_order_amount)                          AS order_amount,
                   sum(available_amount)                            AS available_amount,
                   sum(revenue)                                     AS revenue,
                   if(order_amount > 0, quantile(median_price), 0)  AS median_price,
                   if(order_amount > 0, revenue / order_amount, 0)  AS avg_bill,
                   product_seller_count_tuple.1                     AS seller_count,
                   product_seller_count_tuple.2                     AS product_count,
                   order_amount / product_count                     AS order_per_product,
                   order_amount / seller_count                      AS order_per_seller,
                   if(order_amount > 0, revenue / product_count, 0) AS revenue_per_product,
                   (SELECT uniq(seller_id), uniq(product_id)
                    FROM kazanex.ke_product_daily_sales
                    WHERE date BETWEEN ? AND ?
                      AND category_id IN
                          if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                             0,
                             dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                             array(?)))                         AS product_seller_count_tuple
            FROM (
                SELECT date,
                       p.product_id,
                       maxMerge(p.max_total_order_amount)                                                               AS max_total_order_amount,
                       minMerge(p.min_total_order_amount)                                                               AS min_total_order_amount,
                       max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                       lagInFrame(maxMerge(p.max_total_order_amount))
                                  over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                       multiIf(min_total_order_amount < max_total_order_amount_delta,
                               daily_order_amount - (max_total_order_amount_delta - min_total_order_amount),
                               max_total_order_amount_delta - max_total_order_amount >= 0,
                               daily_order_amount + (max_total_order_amount_delta - max_total_order_amount),
                               max_total_order_amount_delta > 0 AND min_total_order_amount > max_total_order_amount_delta,
                               daily_order_amount + (min_total_order_amount - max_total_order_amount_delta),
                               daily_order_amount)                                                                      AS order_amount_with_gaps,
                       if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS final_order_amount,
                       median_price * final_order_amount                                                                AS revenue,
                       minMerge(p.min_available_amount)                                                                 AS available_amount,
                       quantileMerge(p.median_price)                                                                    AS median_price,
                       anyLast(seller_id)                                                                               AS seller_id
                FROM kazanex.ke_product_daily_sales p
                WHERE date BETWEEN ? AND ?
                  AND category_id IN
                      if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                         0,
                         dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                         array(?))
                GROUP BY product_id, date
            )
        """
        const val CATEGORY_DAILY_ANALYTICS_SQL = """
            SELECT date,
                   sum(final_order_amount)                          AS order_amount,
                   if(order_amount <= 0, 0, revenue / order_amount) AS average_bill,
                   sum(available_amount)                            AS available_amount,
                   if(sum(revenue) <= 0, 0, sum(revenue))           AS revenue
            FROM (
                     SELECT date,
                            p.product_id,
                            maxMerge(p.max_total_order_amount)                                                               AS max_total_order_amount,
                            minMerge(p.min_total_order_amount)                                                               AS min_total_order_amount,
                            max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                            lagInFrame(maxMerge(p.max_total_order_amount))
                                       over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                            multiIf(min_total_order_amount < max_total_order_amount_delta,
                                    daily_order_amount - (max_total_order_amount_delta - min_total_order_amount),
                                    max_total_order_amount_delta - max_total_order_amount >= 0,
                                    daily_order_amount + (max_total_order_amount_delta - max_total_order_amount),
                                    max_total_order_amount_delta > 0 AND
                                    min_total_order_amount > max_total_order_amount_delta,
                                    daily_order_amount + (min_total_order_amount - max_total_order_amount_delta),
                                    daily_order_amount)                                                                      AS order_amount_with_gaps,
                            if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS final_order_amount,
                            median_price * final_order_amount                                                                AS revenue,
                            minMerge(p.min_available_amount)                                                                 AS available_amount,
                            quantileMerge(p.median_price)                                                                    AS median_price
                     FROM kazanex.ke_product_daily_sales p
                     WHERE date BETWEEN ? AND ?
                       AND category_id IN
                           if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                              0,
                              dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                              array(?))
                     GROUP BY product_id, date
                     )
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
        const val GET_CATEGORY_PRODUCT_ANALYTICS_SQL = """
            SELECT product_id,
                   anyLastMerge(title)                                                 AS title,
                   maxMerge(max_total_order_amount) - minMerge(min_total_order_amount) AS order_amount,
                   median_price * order_amount                                         AS revenue,
                   quantileMerge(median_price)                                         AS median_price,
                   anyLastMerge(available_amount)                                      AS available_amount,
                   anyLastMerge(reviews_amount)                                        AS reviews_amount,
                   anyLastMerge(photo_key)                                             AS photo_key,
                   anyLastMerge(rating)                                                AS rating,
                   count() OVER()                                                      AS total_row_count
            FROM %s
            WHERE category_id IN
                  if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                     0,
                     dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                     array(?))
              AND (date = ?)
            GROUP BY product_id
        """
        const val GET_PRODUCTS_ORDER_CHART_SQL = """
            SELECT product_id,
                   groupArray(order_amount) AS order_amount_chart
            FROM (
                     SELECT date,
                            product_id,
                            maxMerge(max_total_order_amount)                AS max_total_order_amount,
                            minMerge(min_total_order_amount)                AS min_total_order_amount,
                            max_total_order_amount - min_total_order_amount AS order_amount
                     FROM kazanex.ke_product_daily_sales
                     WHERE product_id IN (?)
                       AND date BETWEEN ? AND ?
                     GROUP BY product_id, date
                     ORDER BY date WITH FILL FROM toDate(?) TO toDate(?)
                  )
            GROUP BY product_id;
        """
    }

    fun getCategoryAnalyticsWithPrev(
        categoryId: Long,
        queryPeriod: QueryPeriod,
    ): ChCategoryAnalyticsPair? {
        val queryTable = when (queryPeriod) {
            QueryPeriod.WEEK -> "kazanex.category_weekly_stats"
            QueryPeriod.TWO_WEEK -> "kazanex.category_two_week_stats"
            QueryPeriod.MONTH -> "kazanex.category_monthly_stats"
            QueryPeriod.TWO_MONTH -> "kazanex.category_two_month_stats"
        }
        val fromDate = when (queryPeriod) {
            QueryPeriod.WEEK -> LocalDate.now().minusDays(7)
            QueryPeriod.TWO_WEEK -> LocalDate.now().minusDays(14)
            QueryPeriod.MONTH -> LocalDate.now().minusDays(30)
            QueryPeriod.TWO_MONTH -> LocalDate.now().minusDays(60)
        }
        val toDate = LocalDate.now()
        val fromDatePrev = when (queryPeriod) {
            QueryPeriod.WEEK -> fromDate.minusDays(7)
            QueryPeriod.TWO_WEEK -> fromDate.minusDays(14)
            QueryPeriod.MONTH -> fromDate.minusDays(30)
            QueryPeriod.TWO_MONTH -> fromDate.minusDays(60)
        }
        val toDatePrev = fromDate
        val aggTableDate = jdbcTemplate.queryForObject(
            "SELECT max(date) AS max_date FROM %s".format(queryTable),
        ) { rs, _ -> rs.getDate("max_date") }?.toLocalDate()
            ?: throw IllegalStateException("Can't determine date for table query")
        val sql = GET_CATEGORIES_ANALYTICS_WITH_PREV_SQL.format(queryTable)

        return jdbcTemplate.queryForObject(
            sql,
            CategoryAnalyticsMapper(),
            fromDate, toDate,
            categoryId, categoryId, categoryId,
            fromDatePrev, toDatePrev,
            categoryId, categoryId, categoryId,
            categoryId, categoryId, categoryId,
            aggTableDate
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

    fun getCategoryProductsAnalytics(
        categoryId: Long,
        queryPeriod: QueryPeriod,
        filter: FilterBy? = null,
        sort: SortBy? = null,
        page: PageLimitOffset,
    ): List<ChCategoryProductsAnalytics> {
        val queryTable = when (queryPeriod) {
            QueryPeriod.WEEK -> "kazanex.category_product_weekly_stats"
            QueryPeriod.TWO_WEEK -> "kazanex.category_product_two_week_stats"
            QueryPeriod.MONTH -> "kazanex.category_product_monthly_stats"
            QueryPeriod.TWO_MONTH -> "kazanex.category_product_two_month_stats"
        }
        val aggTableDate = jdbcTemplate.queryForObject(
            "SELECT max(date) AS max_date FROM %s".format(queryTable),
        ) { rs, _ -> rs.getDate("max_date") }?.toLocalDate()
            ?: throw IllegalStateException("Can't determine date for table query")
        val sqlStringBuilder = StringBuilder()
        sqlStringBuilder.append(GET_CATEGORY_PRODUCT_ANALYTICS_SQL.format(queryTable))
        filter?.sqlFilterFields?.forEachIndexed { index, sqlFilterField ->
            if (index == 0) {
                sqlStringBuilder.append("HAVING ${sqlFilterField.sqlPredicate()} ")
            }
            sqlStringBuilder.append("AND ${sqlFilterField.sqlPredicate()} ")
        }
        if (sort != null && sort.sortFields.isNotEmpty()) {
            sqlStringBuilder.append("ORDER BY ")
            sort.sortFields.forEachIndexed { index, sortField ->
                if (index >= sort.sortFields.size - 1) {
                    sqlStringBuilder.append("${sortField.fieldName} ${sortField.order.name}")
                } else {
                    sqlStringBuilder.append("${sortField.fieldName} ${sortField.order.name},")
                }
            }
        }
        sqlStringBuilder.append("LIMIT ${page.offset},${page.limit}")

        log.debug { "Get category products analytics SQL: $sqlStringBuilder" }

        return jdbcTemplate.query(
            sqlStringBuilder.toString(),
            CategoryProductsAnalyticsMapper(),
            categoryId, categoryId, categoryId, aggTableDate
        )
    }

    fun getProductsOrderChart(
        productIds: List<String>,
        fromDate: LocalDate,
        toDate: LocalDate
    ): List<ChCategoryProductOrderChart> {
        return jdbcTemplate.query(
            GET_PRODUCTS_ORDER_CHART_SQL,
            CategoryProductOrderChartRowMapper(),
            productIds.toTypedArray(), fromDate, toDate, fromDate, toDate
        )
    }

    fun getCategoryTitle(categoryId: Long): String? {
        return jdbcTemplate.queryForObject(
            "SELECT dictGet('kazanex.categories_hierarchical_dictionary', ('title'), ?) AS category_title\n" +
                    "FROM system.numbers LIMIT 1",
            { rs, _ -> rs.getString("category_title") },
            categoryId,
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
