package dev.crashteam.keanalytics.job

import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import dev.crashteam.keanalytics.service.AggregateJobService
import dev.crashteam.keanalytics.service.model.StatType
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.quartz.DisallowConcurrentExecution
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.springframework.jdbc.core.JdbcTemplate

private val log = KotlinLogging.logger {}

@DisallowConcurrentExecution
class AggregateStatsJob : Job {

    private lateinit var jdbcTemplate: JdbcTemplate

    private lateinit var aggregateJobService: AggregateJobService

    private lateinit var chCategoryRepository: CHCategoryRepository

    override fun execute(context: JobExecutionContext) {
        val applicationContext = context.getApplicationContext()
        jdbcTemplate = applicationContext.getBean("clickHouseJdbcTemplate") as JdbcTemplate
        chCategoryRepository = applicationContext.getBean(CHCategoryRepository::class.java)
        aggregateJobService = applicationContext.getBean(AggregateJobService::class.java)
        val rootCategoryIds = chCategoryRepository.getDescendantCategories(0, 1)!!
        runBlocking {
            val firstTask = async {
                try {
                    insertAggregateStats(
                        rootCategoryIds,
                        { statType -> getTableNameForAggCategoryProductsStatsByStatType(statType) }
                    ) { rootCategoryId, statType -> buildCategoryProductsAnalyticsStatSql(rootCategoryId, statType) }
                } catch (e: Exception) {
                    log.error(e) { "Can't insert aggregate stats for category products" }
                }
            }
            val secondTask = async {
                try {
                    insertAggregateStats(
                        rootCategoryIds,
                        { statType -> getTableNameForAggCategoryStatsByStatType(statType) },
                        { rootCategoryId, statType -> buildInsertCategoryAnalyticsStatSql(rootCategoryId, statType) })
                } catch (e: Exception) {
                    log.error(e) { "Can't insert aggregate stats for category" }
                }
            }
            awaitAll(firstTask, secondTask)
        }
    }

    private fun insertAggregateStats(
        rootCategoryIds: List<Long>,
        tableDetermineBlock: (statType: StatType) -> String,
        buildSqlBlock: (rootCategoryId: Long, statType: StatType) -> String,
    ) {
        for (statType in StatType.values()) {
            val tableName = tableDetermineBlock(statType)
            for (rootCategoryId in rootCategoryIds) {
                val isAlreadyExists =
                    aggregateJobService.checkCategoryAlreadyAggregated(tableName, rootCategoryId, statType)
                if (!isAlreadyExists) {
                    val insertStatSql = buildSqlBlock(rootCategoryId, statType)
                    log.debug { "Insert aggregate table sql: $insertStatSql" }
                    log.info { "Execute insert aggregation stats for table `$tableName`. categoryId=$rootCategoryId" }
                    jdbcTemplate.execute(insertStatSql)
                    aggregateJobService.putCategoryAggregate(tableName, rootCategoryId, statType)
                }
            }
        }
    }

    private fun buildCategoryProductsAnalyticsStatSql(categoryId: Long, statType: StatType): String {
        val datePredicate = getPeriodFromStatTypeWithColumName("timestamp", statType)
        val tableName = getTableNameForAggCategoryProductsStatsByStatType(statType)
        return INSERT_AGG_CATEGORY_PRODUCTS_STATS_SQL.format(
            tableName,
            datePredicate,
            categoryId,
            categoryId,
            categoryId
        )
    }

    private fun buildInsertCategoryAnalyticsStatSql(categoryId: Long, statType: StatType): String {
        val sqlPeriod = getPeriodFromStatType(statType)
        val prevSqlPeriod = when (statType) {
            StatType.WEEK -> "toDate(now()) - 14"
            StatType.TWO_WEEK -> "toDate(now()) - 28"
            StatType.MONTH -> "toDate(now()) - 60"
            StatType.TWO_MONTH -> "toDate(now()) - 120"
        }
        val tableName = getTableNameForAggCategoryStatsByStatType(statType)
        return INSERT_AGG_CATEGORY_STATS_SQL.format(
            tableName,
            sqlPeriod, "toDate(now())",
            categoryId, categoryId, categoryId,
            sqlPeriod, "toDate(now())",
            categoryId, categoryId, categoryId,
            prevSqlPeriod, sqlPeriod,
            categoryId, categoryId, categoryId,
            prevSqlPeriod, sqlPeriod,
            categoryId, categoryId, categoryId,
        )
    }

    private fun getTableNameForAggCategoryProductsStatsByStatType(statType: StatType) = when (statType) {
        StatType.WEEK -> "kazanex.category_product_weekly_stats"
        StatType.TWO_WEEK -> "kazanex.category_product_two_week_stats"
        StatType.MONTH -> "kazanex.category_product_monthly_stats"
        StatType.TWO_MONTH -> "kazanex.category_product_two_month_stats"
    }

    private fun getTableNameForAggCategoryStatsByStatType(statType: StatType) = when (statType) {
        StatType.WEEK -> "kazanex.category_weekly_stats"
        StatType.TWO_WEEK -> "kazanex.category_two_week_stats"
        StatType.MONTH -> "kazanex.category_monthly_stats"
        StatType.TWO_MONTH -> "kazanex.category_two_month_stats"
    }

    private fun getPeriodFromStatTypeWithColumName(dateColumnName: String, statType: StatType) = when (statType) {
        StatType.WEEK -> "$dateColumnName >= toDate(now()) - 7"
        StatType.TWO_WEEK -> "$dateColumnName >= toDate(now()) - 14"
        StatType.MONTH -> "$dateColumnName >= toDate(now()) - 30"
        StatType.TWO_MONTH -> "$dateColumnName >= toDate(now()) - 60"
    }

    private fun getPeriodFromStatType(statType: StatType) = when (statType) {
        StatType.WEEK -> "toDate(now()) - 7"
        StatType.TWO_WEEK -> "toDate(now()) - 14"
        StatType.MONTH -> "toDate(now()) - 30"
        StatType.TWO_MONTH -> "toDate(now()) - 60"
    }

    companion object {
        private const val MAX_DATE_AGG_STATS_SQL = """
            SELECT max(date) AS max_date FROM %s
        """
        private const val INSERT_AGG_CATEGORY_PRODUCTS_STATS_SQL = """
            INSERT INTO %s
            SELECT date,
                   latest_category_id                  AS category_id,
                   product_id,
                   anyLastState(title)                 AS title,
                   maxState(total_orders_amount)       AS max_total_order_amount,
                   minState(total_orders_amount)       AS min_total_order_amount,
                   quantileState(purchase_price)       AS median_price,
                   anyLastState(last_available_amount) AS available_amount,
                   anyLastState(last_reviews_amount)   AS reviews_amount,
                   anyLastState(photo_key)             AS photo_key,
                   anyLastState(last_rating)           AS rating
            FROM (
                     SELECT latest_category_id,
                            product_id,
                            title,
                            toInt64(total_orders_amount)                                                                 AS total_orders_amount,
                            toInt64(purchase_price)                                                                      AS purchase_price,
                            photo_key,
                            last_value(rating) OVER (PARTITION BY latest_category_id, product_id ORDER BY timestamp ASC) AS last_rating,
                            last_value(reviews_amount)
                                       OVER (PARTITION BY latest_category_id, product_id ORDER BY timestamp ASC)         AS last_reviews_amount,
                            last_value(total_available_amount)
                                       OVER (PARTITION BY latest_category_id, product_id ORDER BY timestamp ASC)         AS last_available_amount
                     FROM kazanex.product
                     WHERE %s
                     AND latest_category_id IN (
                         if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0)) > 0,
                            dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0),
                            array(%s))
                         )
                     )
            GROUP BY category_id, product_id, toStartOfDay(now()) as date
        """
        private const val INSERT_AGG_CATEGORY_STATS_SQL = """
            INSERT INTO %s
            SELECT toStartOfDay(now()) as date,
                   c.category_id,
                   c.order_amount,
                   c.available_amount,
                   c.revenue,
                   c.median_price,
                   c.avg_bill,
                   c.seller_count,
                   c.product_count,
                   c.order_per_product,
                   c.order_per_seller,
                   c.revenue_per_product,
                   p.prev_order_amount,
                   p.prev_available_amount,
                   p.prev_revenue,
                   p.prev_median_price,
                   p.prev_avg_bill,
                   p.prev_seller_count,
                   p.prev_product_count,
                   p.prev_order_per_product,
                   p.prev_order_per_seller,
                   p.prev_revenue_per_product
            FROM (
                     SELECT category_id                                      AS category_id,
                            sum(final_order_amount)                          AS order_amount,
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
                             WHERE date BETWEEN %s AND %s
                               AND category_id IN
                                   if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0)) >
                                      0,
                                      dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0),
                                      array(%s)))                         AS product_seller_count_tuple
                     FROM (
                              SELECT date,
                                     anyLast(p.latest_category_id)                                                                    AS category_id,
                                     p.product_id,
                                     max(p.total_orders_amount)                                                                       AS max_total_order_amount,
                                     min(p.total_orders_amount)                                                                       AS min_total_order_amount,
                                     max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                                     lagInFrame(max(p.total_orders_amount))
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
                                     min(p.total_available_amount)                                                                    AS available_amount,
                                     quantile(p.purchase_price)                                                                       AS median_price,
                                     anyLast(seller_id)                                                                               AS seller_id
                              FROM kazanex.product p
                              WHERE timestamp BETWEEN %s AND %s
                                AND latest_category_id IN
                                    if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0)) >
                                       0,
                                       dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0),
                                       array(%s))
                              GROUP BY product_id, toDate(timestamp) AS date
                              )
                     GROUP BY category_id
                     ) AS c
            JOIN (
                SELECT category_id                                                     AS category_id,
                       sum(final_order_amount)                                         AS prev_order_amount,
                       sum(available_amount)                                           AS prev_available_amount,
                       sum(revenue)                                                    AS prev_revenue,
                       if(prev_order_amount > 0, quantile(median_price), 0)            AS prev_median_price,
                       if(prev_order_amount > 0, prev_revenue / prev_order_amount, 0)  AS prev_avg_bill,
                       prev_product_seller_count_tuple.1                               AS prev_seller_count,
                       prev_product_seller_count_tuple.2                               AS prev_product_count,
                       prev_order_amount / prev_product_count                          AS prev_order_per_product,
                       prev_order_amount / prev_seller_count                           AS prev_order_per_seller,
                       if(prev_order_amount > 0, prev_revenue / prev_product_count, 0) AS prev_revenue_per_product,
                       (SELECT uniq(seller_id), uniq(product_id)
                        FROM kazanex.ke_product_daily_sales
                        WHERE date BETWEEN %s AND %s
                          AND category_id IN
                              if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0)) >
                                 0,
                                 dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0),
                                 array(%s)))                                        AS prev_product_seller_count_tuple
                FROM (
                         SELECT date,
                                anyLast(p.latest_category_id)                                                                    AS category_id,
                                p.product_id,
                                max(p.total_orders_amount)                                                                       AS max_total_order_amount,
                                min(p.total_orders_amount)                                                                       AS min_total_order_amount,
                                max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                                lagInFrame(max(p.total_orders_amount))
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
                                min(p.total_available_amount)                                                                    AS available_amount,
                                quantile(p.purchase_price)                                                                       AS median_price,
                                anyLast(seller_id)                                                                               AS seller_id
                         FROM kazanex.product p
                         WHERE timestamp BETWEEN %s AND %s
                           AND latest_category_id IN
                               if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0)) >
                                  0,
                                  dictGetDescendants('kazanex.categories_hierarchical_dictionary', %s, 0),
                                  array(%s))
                         GROUP BY product_id, toDate(timestamp) AS date
                         ) AS p
                GROUP BY category_id
            ) AS p ON
                c.category_id = p.category_id
        """
    }
}
