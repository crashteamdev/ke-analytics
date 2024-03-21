package dev.crashteam.keanalytics.job

import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import mu.KotlinLogging
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate

private val log = KotlinLogging.logger {}

class AggregateStatsJob : Job {

    override fun execute(context: JobExecutionContext) {
        val applicationContext = context.getApplicationContext()
        val jdbcTemplate = applicationContext.getBean("clickHouseJdbcTemplate") as JdbcTemplate
        val chCategoryRepository = applicationContext.getBean(CHCategoryRepository::class.java)
        val rootCategoryIds = chCategoryRepository.getDescendantCategories(0, 1)!!
        for (statType in StatType.values()) {
            val tableName = getTableNameFromStatType(statType)
            val aggTableLastUpdateDate = jdbcTemplate.queryForObject(
                MAX_DATE_AGG_STATS_SQL.format(tableName),
            ) { rs, _ -> rs.getDate("max_date") }
            log.info { "Aggregate table `$tableName` last update date `$aggTableLastUpdateDate`" }

            if (aggTableLastUpdateDate?.toLocalDate() == LocalDate.now()) continue

            rootCategoryIds.forEach { rootCategoryId ->
                val insertStatSql = buildInsertStatSql(rootCategoryId, statType)
                log.info { "Execute insert aggregation stats. categoryId=$rootCategoryId" }
                jdbcTemplate.execute(insertStatSql)
            }
        }
    }

    private fun buildInsertStatSql(categoryId: Long, statType: StatType): String {
        val datePredicate = getPeriodFromStatType(statType)
        val table = getTableNameFromStatType(statType)
        return INSERT_AGG_STATS_SQL.format(table, datePredicate, categoryId, categoryId, categoryId)
    }

    private fun getTableNameFromStatType(statType: StatType) = when (statType) {
        StatType.WEEK -> "kazanex.category_product_weekly_stats"
        StatType.TWO_WEEK -> "kazanex.category_product_two_week_stats"
        StatType.MONTH -> "kazanex.category_product_monthly_stats"
        StatType.TWO_MONTH -> "kazanex.category_product_two_month_stats"
    }

    private fun getPeriodFromStatType(statType: StatType) = when (statType) {
        StatType.WEEK -> "timestamp >= toDate(now()) - 7"
        StatType.TWO_WEEK -> "timestamp >= toDate(now()) - 14"
        StatType.MONTH -> "timestamp >= toDate(now()) - 30"
        StatType.TWO_MONTH -> "timestamp >= toDate(now()) - 60"
    }

    private enum class StatType {
        WEEK,
        TWO_WEEK,
        MONTH,
        TWO_MONTH,
    }

    companion object {
        private const val MAX_DATE_AGG_STATS_SQL = """
            SELECT max(date) AS max_date FROM %s
        """
        private const val INSERT_AGG_STATS_SQL = """
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
    }
}
