package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.config.RedisConfig
import dev.crashteam.keanalytics.extensions.toLocalDates
import dev.crashteam.keanalytics.extensions.toMoney
import dev.crashteam.keanalytics.extensions.toRepositoryDomain
import dev.crashteam.keanalytics.math.MathUtils
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.*
import dev.crashteam.keanalytics.service.model.*
import dev.crashteam.mp.base.DatePeriod
import dev.crashteam.mp.base.Filter
import dev.crashteam.mp.base.LimitOffsetPagination
import dev.crashteam.mp.base.Sort
import dev.crashteam.mp.external.analytics.category.ProductAnalytics
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.cache.annotation.Cacheable
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Service
import java.math.RoundingMode
import java.time.LocalDate

private val log = KotlinLogging.logger {}

@Service
class CategoryAnalyticsService(
    private val chCategoryRepository: CHCategoryRepository,
    private val conversionService: ConversionService,
) {

    @Cacheable(
        value = [RedisConfig.EXTERNAL_CATEGORY_ANALYTICS_CACHE_NAME],
        key = "{#datePeriod}",
        unless = "#result == null || #result.categoryAnalytics.isEmpty()"
    )
    suspend fun getRootCategoryAnalytics(
        datePeriod: DatePeriod,
    ): CategoryAnalyticsCacheableWrapper {
        return withContext(Dispatchers.IO) {
            try {
                log.debug {
                    "Get root categories analytics (Async). queryPeriod=$datePeriod;"
                }
                val rootCategoryIds = chCategoryRepository.getDescendantCategories(0, 1)
                log.debug { "Root categories: $rootCategoryIds" }
                val categoryAnalyticsInfoList = rootCategoryIds?.map { rootCategoryId ->
                    async {
                        calculateCategoryAnalytics(rootCategoryId, datePeriod)
                    }
                }?.awaitAll()
                log.debug {
                    "Finish get root categories analytics (Async)." +
                            " queryPeriod=$datePeriod" +
                            " resultSize=${categoryAnalyticsInfoList?.size}"
                }
                if (categoryAnalyticsInfoList == null) {
                    return@withContext CategoryAnalyticsCacheableWrapper(emptyList())
                }

                CategoryAnalyticsCacheableWrapper(categoryAnalyticsInfoList)
            } catch (e: Exception) {
                log.error(e) {
                    "Exception during get root categories analytics." +
                            " queryPeriod=$datePeriod;"
                }
                CategoryAnalyticsCacheableWrapper(emptyList())
            }
        }
    }

    @Cacheable(
        value = [RedisConfig.EXTERNAL_CATEGORY_ANALYTICS_CACHE_NAME],
        key = "{#categoryId, #datePeriod}",
        unless = "#result == null || #result.categoryAnalytics.isEmpty()"
    )
    suspend fun getCategoryAnalytics(
        categoryId: Long,
        datePeriod: DatePeriod,
    ): CategoryAnalyticsCacheableWrapper {
        return withContext(Dispatchers.IO) {
            try {
                log.debug {
                    "Get category analytics (Async)." +
                            " categoryId=$categoryId; queryPeriod=$datePeriod;"
                }
                val childrenCategoryIds = chCategoryRepository.getDescendantCategories(categoryId, 1)
                log.debug { "Child categories: $childrenCategoryIds" }
                val categoryAnalyticsInfoList = childrenCategoryIds?.map { categoryId ->
                    async {
                        calculateCategoryAnalytics(categoryId, datePeriod)
                    }
                }?.awaitAll()
                log.debug {
                    "Finish get category analytics." +
                            " categoryId=$categoryId; queryPeriod=$datePeriod" +
                            " resultSize=${categoryAnalyticsInfoList?.size}"
                }
                if (categoryAnalyticsInfoList == null) {
                    return@withContext CategoryAnalyticsCacheableWrapper(emptyList())
                }

                CategoryAnalyticsCacheableWrapper(categoryAnalyticsInfoList)
            } catch (e: Exception) {
                log.error(e) {
                    "Exception during get categories analytics." +
                            " categoryId=$categoryId; queryPeriod=$datePeriod;"
                }
                CategoryAnalyticsCacheableWrapper(emptyList())
            }
        }
    }

    fun getCategoryDailyAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate
    ): List<CategoryDailyAnalytics> {
        return chCategoryRepository.getCategoryDailyAnalytics(categoryId, fromTime, toTime)
            .map { categoryDailyAnalytics ->
                CategoryDailyAnalytics(
                    date = categoryDailyAnalytics.date,
                    revenue = categoryDailyAnalytics.revenue.setScale(2, RoundingMode.HALF_UP),
                    salesCount = categoryDailyAnalytics.orderAmount,
                    availableCount = categoryDailyAnalytics.availableAmount,
                    averageBill = categoryDailyAnalytics.averageBill.setScale(2, RoundingMode.HALF_UP)
                )
            }
    }

    fun getCategoryProductsAnalytics(
        categoryId: Long,
        datePeriod: DatePeriod,
        filter: List<Filter>? = null,
        sort: List<Sort>? = null,
        page: LimitOffsetPagination
    ): List<ProductAnalytics> {
        val filterSql = filter?.let {
            FilterBy(
                sqlFilterFields = filter.map {
                    conversionService.convert(it, SqlFilterField::class.java)!!
                }
            )
        }
        val categoryProductsAnalytics = chCategoryRepository.getCategoryProductsAnalytics(
            categoryId = categoryId,
            queryPeriod = mapDatePeriodToQueryPeriod(datePeriod),
            filter = filterSql,
            sort = sort?.let {
                SortBy(
                    sortFields = sort.map {
                        SortField(
                            fieldName = it.fieldName,
                            order = it.order.toRepositoryDomain()
                        )
                    }
                )
            },
            page = PageLimitOffset(
                limit = page.limit.toInt(),
                offset = page.offset.toInt()
            )
        )
        val productIds = categoryProductsAnalytics.map { it.productId }.distinct()
        val periodLocalDate = datePeriod.toLocalDates()
        val productsOrderChart =
            chCategoryRepository.getProductsOrderChart(productIds, periodLocalDate.fromDate, periodLocalDate.toDate)
                .associate { it.productId to it.orderChart }
        return categoryProductsAnalytics.map {
            ProductAnalytics.newBuilder().apply {
                this.productId = it.productId
                this.name = it.title
                this.revenue = it.revenue.toMoney()
                this.price = it.medianPrice.toMoney()
                this.salesCount = it.orderAmount
                this.rating = it.rating.setScale(1, RoundingMode.HALF_UP).toDouble()
                this.reviewsCount = it.reviewsAmount
                this.availableCount = it.availableAmount
                this.photoKey = it.photoKey
                this.addAllSalesChart(productsOrderChart[it.productId])
            }.build()
        }
    }

    fun getCategoryInfo(categoryId: Long): ChCategoryHierarchy? {
        return chCategoryRepository.getCategoryHierarchy(categoryId)
    }

    private suspend fun calculateCategoryAnalytics(
        categoryId: Long,
        datePeriod: DatePeriod,
    ): CategoryAnalyticsInfo = coroutineScope {
        log.debug { "Calculate category analytics queryPeriod=$datePeriod" }
        val categoryAnalyticsTask = async {
            chCategoryRepository.getCategoryAnalyticsWithPrev(
                categoryId = categoryId,
                queryPeriod = mapDatePeriodToQueryPeriod(datePeriod)
            )
        }
        val categoryHierarchyTask = async {
            chCategoryRepository.getCategoryHierarchy(categoryId)!!
        }
        val chCategoryAnalyticsPair = categoryAnalyticsTask.await()!!
        val chCategoryHierarchy = categoryHierarchyTask.await()

        CategoryAnalyticsInfo(
            category = Category(
                categoryId = categoryId,
                name = chCategoryHierarchy.name,
                parentId = chCategoryHierarchy.parentId,
                childrenIds = chCategoryHierarchy.childrenIds
            ),
            analytics = mapCategoryAnalytics(chCategoryAnalyticsPair),
            analyticsPrevPeriod = mapPrevCategoryAnalytics(chCategoryAnalyticsPair),
            analyticsDifference = mapCategoryAnalyticsDifference(chCategoryAnalyticsPair),
        )
    }

    fun sortCategoryAnalytics(
        categoryAnalytics: List<CategoryAnalyticsInfo>,
        sortBy: SortBy
    ): List<CategoryAnalyticsInfo> {
        val comparators = sortBy.sortFields.map { sortField ->
            when (sortField.fieldName) {
                "order_amount" -> compareBy<CategoryAnalyticsInfo> { it.analytics.salesCount }
                "revenue" -> compareBy { it.analytics.revenue }
                "average_bill" -> compareBy { it.analytics.averageBill }
                "seller_count" -> compareBy { it.analytics.sellerCount }
                "product_count" -> compareBy { it.analytics.productCount }
                "order_per_product" -> compareBy { it.analytics.tsts }
                "order_per_seller" -> compareBy { it.analytics.tstc }
                "revenue_per_product" -> compareBy { it.analytics.revenuePerProduct }
                else -> throw IllegalArgumentException("Unknown field name: ${sortField.fieldName}")
            }.let { comparator ->
                if (sortField.order == SortOrder.DESC) comparator.reversed() else comparator
            }
        }

        return categoryAnalytics.sortedWith(comparators.reduce { acc, comparator -> acc.then(comparator) })
    }

    private fun mapCategoryAnalyticsDifference(
        categoryAnalytics: ChCategoryAnalyticsPair
    ): CategoryAnalyticsDifference {
        return CategoryAnalyticsDifference(
            revenuePercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevRevenue,
                categoryAnalytics.revenue
            ).setScale(1, RoundingMode.DOWN),
            revenuePerProductPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevRevenuePerProduct,
                categoryAnalytics.revenuePerProduct
            ).setScale(1, RoundingMode.DOWN),
            salesCountPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevOrderAmount,
                categoryAnalytics.orderAmount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            productCountPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevProductCount,
                categoryAnalytics.productCount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            sellerCountPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevSellerCount,
                categoryAnalytics.sellerCount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            averageBillPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevAvgBill,
                categoryAnalytics.avgBill
            ).setScale(1, RoundingMode.DOWN),
            tstcPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevOrderPerSeller,
                categoryAnalytics.orderPerSeller
            ).setScale(1, RoundingMode.DOWN),
            tstsPercentage = MathUtils.percentageDifference(
                categoryAnalytics.prevOrderPerProduct,
                categoryAnalytics.orderPerProduct
            ).setScale(1, RoundingMode.DOWN)
        )
    }

    private fun mapCategoryAnalytics(categoryAnalytics: ChCategoryAnalyticsPair): CategoryAnalytics {
        return CategoryAnalytics(
            revenue = categoryAnalytics.revenue.setScale(2, RoundingMode.HALF_UP),
            revenuePerProduct = categoryAnalytics.revenuePerProduct.setScale(2, RoundingMode.HALF_UP),
            salesCount = categoryAnalytics.orderAmount,
            productCount = categoryAnalytics.productCount,
            sellerCount = categoryAnalytics.sellerCount,
            averageBill = categoryAnalytics.avgBill.setScale(2, RoundingMode.HALF_UP),
            tsts = categoryAnalytics.orderPerProduct.setScale(2, RoundingMode.HALF_UP),
            tstc = categoryAnalytics.orderPerSeller.setScale(2, RoundingMode.HALF_UP),
        )
    }

    private fun mapPrevCategoryAnalytics(categoryAnalytics: ChCategoryAnalyticsPair): CategoryAnalytics {
        return CategoryAnalytics(
            revenue = categoryAnalytics.prevRevenue.setScale(2, RoundingMode.HALF_UP),
            revenuePerProduct = categoryAnalytics.prevRevenuePerProduct.setScale(2, RoundingMode.HALF_UP),
            salesCount = categoryAnalytics.prevOrderAmount,
            productCount = categoryAnalytics.prevProductCount,
            sellerCount = categoryAnalytics.prevSellerCount,
            averageBill = categoryAnalytics.prevAvgBill.setScale(2, RoundingMode.HALF_UP),
            tsts = categoryAnalytics.prevOrderPerProduct.setScale(2, RoundingMode.HALF_UP),
            tstc = categoryAnalytics.prevOrderPerSeller.setScale(2, RoundingMode.HALF_UP),
        )
    }

    private fun mapDatePeriodToQueryPeriod(source: DatePeriod): QueryPeriod {
        return when (source) {
            DatePeriod.DATE_PERIOD_UNSPECIFIED, DatePeriod.UNRECOGNIZED -> QueryPeriod.MONTH
            DatePeriod.DATE_PERIOD_WEEK -> QueryPeriod.WEEK
            DatePeriod.DATE_PERIOD_TWO_WEEK -> QueryPeriod.TWO_WEEK
            DatePeriod.DATE_PERIOD_MONTH -> QueryPeriod.MONTH
            DatePeriod.DATE_PERIOD_TWO_MONTH -> QueryPeriod.TWO_MONTH
        }
    }
}
