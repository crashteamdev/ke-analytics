package dev.crashteam.keanalytics.repository.clickhouse

import dev.crashteam.keanalytics.repository.clickhouse.mapper.*
import dev.crashteam.keanalytics.repository.clickhouse.model.*
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.queryForObject
import org.springframework.stereotype.Repository
import ru.yandex.clickhouse.ClickHouseArray
import ru.yandex.clickhouse.domain.ClickHouseDataType
import java.sql.PreparedStatement
import java.sql.Types
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.stream.Collectors

@Repository
class CHProductRepository(
    @Qualifier("clickHouseJdbcTemplate") private val jdbcTemplate: JdbcTemplate
) {

    private companion object {
        private const val GET_PRODUCT_ADDITIONAL_INFO_SQL = """
            SELECT min(timestamp) as first_discovered
                FROM kazanex.product
            WHERE product_id = ?
              AND sku_id = ?
            GROUP BY product_id, sku_id
        """
        const val GET_PRODUCT_HISTORY_SQL = """
            SELECT date,
                   product_id,
                   sku_id,
                   title,
                   if(available_amount_diff < 0 OR available_amount_diff > 0 AND total_orders_amount_diff = 0,
                      total_orders_amount_diff, available_amount_diff)                  AS order_amount,
                   reviews_amount_diff                                                  AS review_amount,
                   full_price / 100 AS full_price,
                   purchase_price / 100 AS purchase_price,
                   photo_key,
                   order_amount * purchase_price AS sales_amount,
                   available_amount AS available_amount,
                   total_available_amount
            FROM (
                 SELECT date,
                        product_id,
                        sku_id,
                        title,
                        available_amount_max - available_amount_min       AS available_amount_diff,
                        last_available_amount                             AS available_amount,
                        total_orders_amount_max - total_orders_amount_min AS total_orders_amount_diff,
                        total_available_amount,
                        reviews_amount_max - reviews_amount_min           AS reviews_amount_diff,
                        purchase_price,
                        full_price,
                        photo_key
                 FROM (
                          SELECT date,
                                 product_id,
                                 sku_id,
                                 any(title)               AS title,
                                 min(available_amount)    AS available_amount_min,
                                 max(available_amount)    AS available_amount_max,
                                 anyLast(available_amount) AS last_available_amount,
                                 min(total_orders_amount) AS total_orders_amount_min,
                                 max(total_orders_amount) AS total_orders_amount_max,
                                 max(total_available_amount) AS total_available_amount,
                                 min(reviews_amount)      AS reviews_amount_min,
                                 max(reviews_amount)      AS reviews_amount_max,
                                 quantile(purchase_price) AS purchase_price,
                                 any(full_price)          AS full_price,
                                 any(photo_key)           AS photo_key
                          FROM kazanex.product
                          WHERE product_id = ?
                            AND sku_id = ?
                            AND timestamp BETWEEN ? AND ?
                          GROUP BY product_id, sku_id, toDate(timestamp) AS date
                          ORDER BY date
                 )
         )
        """
        private const val GET_PRODUCTS_SALES = """
            WITH product_sales AS
                (SELECT product_id,
                        title,
                        final_order_amount                    AS order_amount,
                        purchase_price / 100,
                        order_amount * (purchase_price / 100) AS sales_amount,
                        seller_title,
                        seller_link,
                        seller_account_id
                 FROM (
                          SELECT date,
                                 product_id,
                                 any(title)                                                                                       AS title,
                                 min(total_orders_amount)                                                                         AS total_orders_amount_min,
                                 max(total_orders_amount)                                                                         AS total_orders_amount_max,
                                 total_orders_amount_max - total_orders_amount_min                                                AS daily_order_amount,
                                 lagInFrame(max(total_orders_amount))
                                            over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                                 multiIf(total_orders_amount_min < max_total_order_amount_delta,
                                         daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                                         max_total_order_amount_delta - total_orders_amount_max >= 0,
                                         daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                                         max_total_order_amount_delta > 0 AND
                                         total_orders_amount_min > max_total_order_amount_delta,
                                         daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                                         daily_order_amount)                                                                      AS order_amount_with_gaps,
                                 if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS final_order_amount,
                                 quantile(purchase_price)                                                                         AS purchase_price,
                                 max(seller_title)                                                                                AS seller_title,
                                 max(seller_link)                                                                                 AS seller_link,
                                 max(seller_account_id)                                                                           AS seller_account_id
                          FROM kazanex.product
                          WHERE product_id IN (?)
                            AND timestamp BETWEEN ? AND ?
                          GROUP BY product_id, toDate(timestamp) AS date
                          ORDER BY date
                          )
                )
                SELECT s.product_id,
                       any(s.title)                                                                       AS title,
                       sum(s.order_amount)                                                                AS order_amount,
                       sum(s.sales_amount)                                                                AS sales_amount,
                       any(s.seller_title)                                                                AS seller_title,
                       any(s.seller_link)                                                                 AS seller_link,
                       any(s.seller_account_id)                                                           AS seller_account_id,
                       sum(s.order_amount) / date_diff('day', toDate(?), toDate(?)) AS daily_order_amount
                FROM product_sales s
                GROUP BY product_id
        """
        private val GET_CATEGORY_OVERALL_INFO = """
            WITH category_products AS (SELECT p.timestamp,
                p.product_id,
                p.sku_id,
                p.latest_category_id,
                p.total_orders_amount,
                p.purchase_price,
                p.seller_id
                FROM kazanex.product p
                WHERE timestamp BETWEEN ? AND ?
                AND latest_category_id IN
                if(length(dictGetDescendants('categories_hierarchical_dictionary', ?, 0)) >
                0,
                dictGetDescendants('categories_hierarchical_dictionary', ?, 0),
                array(?)))

            SELECT round((sum(price) / 100) / count(), 2)          AS avg_price,
                   sum(revenue) / 100                              AS revenue,
                   sum(order_amount)                               AS order_count,
                   any(seller_count)                               AS seller_counts,
                   any(product_count)                              AS product_counts,
                   round(sum(order_amount) / any(seller_count), 3) AS sales_per_seller,
                   (SELECT count()
                    FROM (
                             SELECT sum(order_amount) AS order_amount
                             FROM (
                                      SELECT total_orders_amount_max - total_orders_amount_min AS order_amount,
                                             seller_identifier                                 AS seller_id
                                      FROM (SELECT min(total_orders_amount) AS total_orders_amount_min,
                                                   max(total_orders_amount) AS total_orders_amount_max,
                                                   max(seller_id)           AS seller_identifier
                                            FROM category_products
                                            GROUP BY product_id)
                                      )
                             GROUP BY seller_id
                             )
                    WHERE order_amount <= 0)                       AS seller_with_zero_sales_count,
                   (SELECT count()
                    FROM (SELECT product_id,
                                 total_orders_amount_max - total_orders_amount_min AS order_amount,
                                 seller_identifier
                          FROM (SELECT product_id,
                                       min(total_orders_amount) AS total_orders_amount_min,
                                       max(total_orders_amount) AS total_orders_amount_max,
                                       max(seller_id)           AS seller_identifier
                                FROM category_products
                                GROUP BY product_id))
                    WHERE order_amount <= 0)                       AS product_zero_sales_count
            FROM (SELECT product_id,
                         total_orders_amount_max - total_orders_amount_min AS order_amount,
                         (total_orders_amount_max - total_orders_amount_min) * purchase_price AS revenue,
                         purchase_price                                    AS price,
                         seller_identifier,
                         (SELECT uniq(seller_id) FROM category_products)   AS seller_count,
                         (SELECT uniq(product_id) FROM category_products)  AS product_count
                  FROM (SELECT product_id,
                               min(total_orders_amount) AS total_orders_amount_min,
                               max(total_orders_amount) AS total_orders_amount_max,
                               quantile(purchase_price) AS purchase_price,
                               max(seller_id)           AS seller_identifier
                        FROM category_products
                        GROUP BY product_id))
            WHERE order_amount > 0
        """.trimIndent()
        private val GET_SELLER_OVERALL_INFO = """
            WITH product_sales AS (
                    SELECT date,
                           product_id,
                           anyLastMerge(title)                                                                              AS title,
                           minMerge(min_total_order_amount)                                                                 AS total_orders_amount_min,
                           maxMerge(max_total_order_amount)                                                                 AS total_orders_amount_max,
                           minMerge(min_available_amount)                                                                   AS available_amount,
                           total_orders_amount_max - total_orders_amount_min                                                AS daily_order_amount,
                           lagInFrame(total_orders_amount_max)
                                      over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                           multiIf(total_orders_amount_min < max_total_order_amount_delta,
                                   daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                                   max_total_order_amount_delta - total_orders_amount_max >= 0,
                                   daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                                   max_total_order_amount_delta > 0 AND
                                   total_orders_amount_min > max_total_order_amount_delta,
                                   daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                                   daily_order_amount)                                                                      AS order_amount_with_gaps,
                           if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS order_amount,
                           quantileMerge(median_price)                                                                      AS purchase_price,
                           order_amount * purchase_price                                                                    AS revenue
                    FROM kazanex.ke_product_daily_sales
                    WHERE seller_link = ?
                      AND date BETWEEN ? AND ?
                    GROUP BY product_id, date
                    ORDER BY date
            )

            SELECT sum(order_amount_sum)                                         AS order_amount,
                   sum(revenue) / 100                                            AS revenue,
                   count(product_id)                                             AS product_count,
                   countIf(order_amount_sum > 0)                                 AS product_with_sales,
                   round((sum(avg_price) / 100) / countIf(order_amount_sum > 0)) AS avg_price,
                   countIf(order_amount_sum <= 0)                                AS product_without_sales
            FROM (
                     SELECT product_id,
                            any(title)               AS title,
                            sum(order_amount)        AS order_amount_sum,
                            sum(revenue)             AS revenue,
                            max(available_amount)    AS last_available_amount,
                            avg(purchase_price)      AS avg_price
                     FROM product_sales s
                     GROUP BY product_id
                     )
        """.trimIndent()
        private val GET_SELLER_ORDER_DYNAMIC = """
            WITH product_sales AS
                (SELECT date,
                        product_id,
                        anyLastMerge(title)                                                                              AS title,
                        minMerge(min_total_order_amount)                                                                 AS total_orders_amount_min,
                        maxMerge(max_total_order_amount)                                                                 AS total_orders_amount_max,
                        minMerge(min_available_amount)                                                                   AS available_amount,
                        total_orders_amount_max - total_orders_amount_min                                                AS daily_order_amount,
                        lagInFrame(total_orders_amount_max)
                                   over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                        multiIf(total_orders_amount_min < max_total_order_amount_delta,
                                daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                                max_total_order_amount_delta - total_orders_amount_max >= 0,
                                daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                                max_total_order_amount_delta > 0 AND
                                total_orders_amount_min > max_total_order_amount_delta,
                                daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                                daily_order_amount)                                                                      AS order_amount_with_gaps,
                        if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS order_amount
                 FROM kazanex.ke_product_daily_sales
                 WHERE seller_link = ?
                   AND date BETWEEN ? AND ?
                 GROUP BY product_id, date
                 ORDER BY date)

            SELECT date, sum(order_amount) AS order_amount FROM product_sales GROUP BY date
        """.trimIndent()
        private val GET_SELLER_SALES_REPORT = """
            SELECT product_id,
                   anyLast(seller_title)                                                                AS seller_title,
                   anyLast(seller_id)                                                                   AS seller_id,
                   anyLast(category_id)                                                                 AS latest_category_id,
                   groupArray(final_order_amount)                                                       AS order_graph,
                   groupArray(available_amount)                                                         AS available_amount_graph,
                   groupArray(median_price)                                                             AS price_graph,
                   anyLast(available_amount)                                                            AS available_amounts,
                   anyLast(median_price)                                                                AS purchase_price,
                   sum(revenue)                                                                         AS sales,
                   (dictGet('kazanex.categories_hierarchical_dictionary', 'title', latest_category_id)) AS category_name,
                   anyLast(title)                                                                       AS name,
                   count() OVER ()                                                                      AS total
            FROM (
                     SELECT date,
                            product_id,
                            anyLast(category_id)                                                                             AS category_id,
                            anyLastMerge(title)                                                                              AS title,
                            minMerge(min_total_order_amount)                                                                 AS total_orders_amount_min,
                            maxMerge(max_total_order_amount)                                                                 AS total_orders_amount_max,
                               minMerge(min_available_amount)                                                                AS available_amount,
                                total_orders_amount_max - total_orders_amount_min                                            AS daily_order_amount,
                            lagInFrame(total_orders_amount_max)
                                       over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                            multiIf(total_orders_amount_min < max_total_order_amount_delta,
                                    daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                                    max_total_order_amount_delta - total_orders_amount_max >= 0,
                                    daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                                    max_total_order_amount_delta > 0 AND
                                    total_orders_amount_min > max_total_order_amount_delta,
                                    daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                                    daily_order_amount)                                                                      AS order_amount_with_gaps,
                            if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS final_order_amount,
                            quantileMerge(median_price)                                                                      AS median_price,
                            maxMerge(seller_title)                                                                           AS seller_title,
                            anyLast(seller_id)                                                                               AS seller_id,
                            final_order_amount * median_price                                                                AS revenue
                     FROM kazanex.ke_product_daily_sales
                     WHERE seller_link = ?
                       AND date BETWEEN ? AND ?
                     GROUP BY product_id, date
                     ORDER BY date
            )
            GROUP BY product_id
            LIMIT ? OFFSET ?
        """.trimIndent()
    }
    private val GET_CATEGORY_SALES_REPORT = """
        SELECT product_id,
               anyLast(seller_title)                                                                AS seller_title,
               anyLast(seller_id)                                                                   AS seller_id,
               anyLast(category_id)                                                                 AS latest_category_id,
               groupArray(order_amount)                                                             AS order_graph,
                  groupArray(available_amount)                                                      AS available_amount_graph,
                   groupArray(median_price)                                                         AS price_graph,
               anyLast(available_amount)                                                            AS available_amounts,
               anyLast(median_price)                                                                AS purchase_price,
               sum(revenue)                                                                         AS sales,
               (dictGet('kazanex.categories_hierarchical_dictionary', 'title', latest_category_id)) AS category_name,
               anyLast(title)                                                                        AS name,
               count() OVER ()                                                                      AS total
        FROM (
            SELECT date,
                   product_id,
                   anyLast(category_id)                                                                             AS category_id,
                   anyLastMerge(title)                                                                              AS title,
                   minMerge(min_total_order_amount)                                                                 AS total_orders_amount_min,
                   maxMerge(max_total_order_amount)                                                                 AS total_orders_amount_max,
                   minMerge(min_available_amount)                                                                   AS available_amount,
                   total_orders_amount_max - total_orders_amount_min                                                AS daily_order_amount,
                   lagInFrame(total_orders_amount_max)
                              over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                   multiIf(total_orders_amount_min < max_total_order_amount_delta,
                           daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                           max_total_order_amount_delta - total_orders_amount_max >= 0,
                           daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                           max_total_order_amount_delta > 0 AND
                           total_orders_amount_min > max_total_order_amount_delta,
                           daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                           daily_order_amount)                                                                      AS order_amount_with_gaps,
                   if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS order_amount,
                   quantileMerge(median_price)                                                                      AS median_price,
                   maxMerge(seller_title)                                                                           AS seller_title,
                   anyLast(seller_id)                                                                               AS seller_id,
                   order_amount * median_price                                                                      AS revenue
            FROM kazanex.ke_product_daily_sales
            WHERE date BETWEEN ? AND ?
              AND ke_product_daily_sales.category_id IN
                  if(length(dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0)) >
                     0,
                     dictGetDescendants('kazanex.categories_hierarchical_dictionary', ?, 0),
                     array(?))
            GROUP BY product_id, date
        )
        GROUP BY product_id
        LIMIT ? OFFSET ?
    """.trimIndent()
    private val GET_PRODUCT_DAILY_ANALYTICS_SQL = """
        SELECT product_id,
               anyLast(title)                   AS title,
               anyLast(category_id)             AS category_id,
               anyLast(seller_link)             AS seller_link,
               anyLast(seller_title)            AS seller_title,
               anyLast(purchase_price)          AS price,
               anyLast(full_price)              AS full_price,
               anyLast(review_amount)           AS review_amount,
               sum(revenue / 100)               AS revenue_sum,
               anyLast(photo_key)               AS photo_key,
               anyLast(rating)                  AS rating,
               groupArray(purchase_price / 100) AS price_chart,
               groupArray(revenue / 100)        AS revenue_chart,
               groupArray(final_order_amount)   AS order_chart,
               groupArray(available_amount)     AS available_chart,
               (SELECT min(date) as first_discovered
                FROM kazanex.ke_product_daily_sales
                WHERE product_id = ?
                GROUP BY product_id)            AS first_discovered
        FROM (
                 SELECT date,
                        product_id,
                        anyLast(category_id)                                                                             AS category_id,
                        anyLastMerge(title)                                                                              AS title,
                        minMerge(min_total_order_amount)                                                                 AS total_orders_amount_min,
                        maxMerge(max_total_order_amount)                                                                 AS total_orders_amount_max,
                        minMerge(min_available_amount)                                                                   AS available_amount,
                        total_orders_amount_max - total_orders_amount_min                                                AS daily_order_amount,
                        lagInFrame(total_orders_amount_max)
                                   over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                        multiIf(total_orders_amount_min < max_total_order_amount_delta,
                                daily_order_amount - (max_total_order_amount_delta - total_orders_amount_min),
                                max_total_order_amount_delta - total_orders_amount_max >= 0,
                                daily_order_amount + (max_total_order_amount_delta - total_orders_amount_max),
                                max_total_order_amount_delta > 0 AND
                                total_orders_amount_min > max_total_order_amount_delta,
                                daily_order_amount + (total_orders_amount_min - max_total_order_amount_delta),
                                daily_order_amount)                                                                      AS order_amount_with_gaps,
                        if(order_amount_with_gaps < 0, 0, order_amount_with_gaps)                                        AS final_order_amount,
                        quantileMerge(median_price)                                                                      AS purchase_price,
                        quantileMerge(median_full_price)                                                                 AS full_price,
                        maxMerge(seller_title)                                                                           AS seller_title,
                        anyLast(seller_link)                                                                             AS seller_link,
                        final_order_amount * purchase_price                                                              AS revenue,
                        anyLastMerge(reviews_amount)                                                                     AS review_amount,
                        anyLastMerge(photo_key)                                                                          AS photo_key,
                        maxMerge(rating)                                                                                 AS rating
                 FROM kazanex.ke_product_daily_sales
                 WHERE product_id = ?
                   AND date BETWEEN ? AND ?
                 GROUP BY product_id, date
                 ORDER BY date
                 )
        GROUP BY product_id
    """.trimIndent()

    fun getProductAdditionalInfo(
        productId: String,
        skuId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): ChProductAdditionalInfo? {
        return jdbcTemplate.queryForObject(
            GET_PRODUCT_ADDITIONAL_INFO_SQL,
            ProductAdditionalInfoMapper(),
            productId, skuId
        )
    }

    fun saveProducts(productFetchList: List<ChKeProduct>) {
        jdbcTemplate.batchUpdate(
            "INSERT INTO kazanex.product " +
                    "(timestamp, product_id, sku_id, title, rating, latest_category_id, reviews_amount," +
                    " total_orders_amount, total_available_amount, available_amount, attributes," +
                    " tags, photo_key, characteristics, seller_id, seller_account_id, seller_title, seller_link," +
                    " seller_registrationDate, seller_rating, seller_reviewsCount, seller_orders, seller_contacts, " +
                    " is_eco, is_adult, full_price, purchase_price)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ProductBatchPreparedStatementSetter(productFetchList)
        )
    }

    fun getProductSales(
        productId: String,
        skuId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductSalesHistory> {
        return jdbcTemplate.query(
            GET_PRODUCT_HISTORY_SQL,
            ProductHistoryStatementSetter(productId, skuId, fromTime, toTime),
            ProductSalesHistoryMapper()
        )
    }

    fun getProductsSales(
        productIds: List<String>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductsSales> {
        return jdbcTemplate.query(
            GET_PRODUCTS_SALES,
            ProductsSalesStatementSetter(productIds, fromTime, toTime),
            ProductsSalesMapper()
        )
    }

    fun getCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): ChCategoryOverallInfo? {
        return jdbcTemplate.queryForObject(
            GET_CATEGORY_OVERALL_INFO,
            CategoryOverallInfoMapper(),
            fromTime, toTime, categoryId, categoryId, categoryId
        )
    }

    fun getSellerAnalytics(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): ChSellerOverallInfo? {
        return jdbcTemplate.queryForObject(
            GET_SELLER_OVERALL_INFO,
            SellerOverallInfoMapper(),
            sellerLink, fromTime.toLocalDate(), toTime.toLocalDate()
        )
    }

    fun getSellerOrderDynamic(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): List<ChSellerOrderDynamic> {
        return jdbcTemplate.query(
            GET_SELLER_ORDER_DYNAMIC,
            SellerOrderDynamicMapper(),
            sellerLink, fromTime.toLocalDate(), toTime.toLocalDate()
        )
    }

    fun getSellerSalesForReport(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): List<ChProductSalesReport> {
        return jdbcTemplate.query(
            GET_SELLER_SALES_REPORT,
            ProductSalesReportMapper(),
            sellerLink, fromTime.toLocalDate(), toTime.toLocalDate(), limit, offset
        )
    }

    fun getCategorySalesForReport(
        categoryId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int
    ): List<ChProductSalesReport> {
        return jdbcTemplate.query(
            GET_CATEGORY_SALES_REPORT,
            ProductSalesReportMapper(),
            fromTime.toLocalDate(), toTime.toLocalDate(), categoryId, categoryId, categoryId, limit, offset
        )
    }

    fun getProductDailyAnalytics(
        productId: String,
        fromDate: LocalDate,
        toDate: LocalDate,
    ): ChProductDailyAnalytics? {
        return jdbcTemplate.queryForObject(
            GET_PRODUCT_DAILY_ANALYTICS_SQL,
            ProductDailyAnalyticsMapper(),
            productId, productId, fromDate, toDate,
        )
    }

    internal class ProductHistoryStatementSetter(
        private val productId: String,
        private val skuId: String,
        private val fromTime: LocalDateTime,
        private val toTime: LocalDateTime,
    ) : PreparedStatementSetter {
        override fun setValues(ps: PreparedStatement) {
            var l = 1
            ps.setString(l++, productId)
            ps.setString(l++, skuId)
            ps.setObject(l++, fromTime)
            ps.setObject(l++, toTime)
        }
    }

    internal class ProductsSalesStatementSetter(
        private val productIds: List<String>,
        private val fromTime: LocalDateTime,
        private val toTime: LocalDateTime
    ) : PreparedStatementSetter {
        override fun setValues(ps: PreparedStatement) {
            var l = 1
            ps.setArray(l++, ClickHouseArray(ClickHouseDataType.String, productIds.toTypedArray()))
            ps.setObject(l++, fromTime)
            ps.setObject(l++, toTime)
            ps.setObject(l++, fromTime.toLocalDate())
            ps.setObject(l++, toTime.toLocalDate())
        }
    }

    internal class ProductBatchPreparedStatementSetter(
        private val products: List<ChKeProduct>
    ) : BatchPreparedStatementSetter {

        override fun setValues(ps: PreparedStatement, i: Int) {
            val product: ChKeProduct = products[i]
            var l = 1
            ps.setObject(
                l++, Instant.ofEpochSecond(
                    product.fetchTime.toEpochSecond(ZoneOffset.UTC)
                ).atZone(ZoneId.of("UTC")).toLocalDateTime()
            )
            ps.setLong(l++, product.productId)
            ps.setLong(l++, product.skuId)
            ps.setString(l++, product.title)
            ps.setObject(l++, product.rating)
            ps.setLong(l++, product.categoryPaths.last())
            ps.setInt(l++, product.reviewsAmount)
            ps.setLong(l++, product.totalOrdersAmount)
            ps.setLong(l++, product.totalAvailableAmount)
            ps.setLong(l++, product.availableAmount)
            ps.setArray(l++, ClickHouseArray(ClickHouseDataType.String, product.attributes.toTypedArray()))
            ps.setArray(l++, ClickHouseArray(ClickHouseDataType.String, product.tags.toTypedArray()))
            ps.setString(l++, product.photoKey)
            ps.setObject(
                l++,
                product.characteristics.stream().collect(Collectors.toMap({ it.type }, { it.title }) { _, u -> u })
            )
            ps.setLong(l++, product.sellerId)
            ps.setLong(l++, product.sellerAccountId)
            ps.setString(l++, product.sellerTitle)
            ps.setString(l++, product.sellerLink)
            ps.setObject(
                l++,
                Instant.ofEpochSecond(product.sellerRegistrationDate / 1000).atZone(ZoneId.of("UTC"))
                    .toLocalDateTime()
            )
            ps.setObject(l++, product.sellerRating)
            ps.setInt(l++, product.sellerReviewsCount)
            ps.setLong(l++, product.sellerOrders)
            ps.setObject(l++, product.sellerContacts)
            ps.setBoolean(l++, product.isEco)
            ps.setBoolean(l++, product.adultCategory)
            product.fullPrice?.let { ps.setLong(l++, product.fullPrice) } ?: ps.setNull(l++, Types.BIGINT)
            ps.setLong(l++, product.purchasePrice)
        }

        override fun getBatchSize(): Int {
            return products.size
        }
    }
}
