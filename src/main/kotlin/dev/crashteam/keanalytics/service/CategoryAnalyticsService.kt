package dev.crashteam.keanalytics.service

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
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Service
import java.math.RoundingMode
import java.time.LocalDate
import java.time.temporal.ChronoUnit

private val log = KotlinLogging.logger {}

@Service
class CategoryAnalyticsService(
    private val chCategoryRepository: CHCategoryRepository,
    private val conversionService: ConversionService,
) {

    suspend fun getRootCategoryAnalytics(
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo>? {
        return withContext(Dispatchers.IO) {
            try {
                log.debug {
                    "Get root categories analytics (Async)." +
                            "fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy"
                }
                val rootCategoryIds = chCategoryRepository.getDescendantCategories(0, 1)
                log.debug { "Root categories: $rootCategoryIds" }
                val categoryAnalyticsInfoList = rootCategoryIds?.map { rootCategoryId ->
                    async {
                        calculateCategoryAnalytics(rootCategoryId, fromTime, toTime)
                    }
                }?.awaitAll()
                log.debug {
                    "Finish get root categories analytics (Async)." +
                            " fromTime=$fromTime; toTime=$toTime;" +
                            " sortBy=$sortBy; resultSize=${categoryAnalyticsInfoList?.size}"
                }
                if (categoryAnalyticsInfoList == null) {
                    return@withContext emptyList()
                }
                val categoryAnalyticsInfos = if (sortBy != null) {
                    sortCategoryAnalytics(categoryAnalyticsInfoList, sortBy)
                } else {
                    categoryAnalyticsInfoList
                }
                categoryAnalyticsInfos
            } catch (e: Exception) {
                log.error(e) {
                    "Exception during get root categories analytics." +
                            " fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy"
                }
                emptyList()
            }
        }
    }

    suspend fun getCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo>? {
        return withContext(Dispatchers.IO) {
            try {
                log.debug {
                    "Get category analytics (Async)." +
                            " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy"
                }
                val childrenCategoryIds = chCategoryRepository.getDescendantCategories(categoryId, 1)
                log.debug { "Child categories: $childrenCategoryIds" }
                val categoryAnalyticsInfoList = childrenCategoryIds?.map { categoryId ->
                    async {
                        calculateCategoryAnalytics(categoryId, fromTime, toTime)
                    }
                }?.awaitAll()
                log.debug {
                    "Finish get category analytics." +
                            " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime;" +
                            " sortBy=$sortBy; resultSize=${categoryAnalyticsInfoList?.size}"
                }
                if (categoryAnalyticsInfoList == null) {
                    return@withContext emptyList()
                }
                if (sortBy != null) {
                    sortCategoryAnalytics(categoryAnalyticsInfoList, sortBy)
                } else {
                    categoryAnalyticsInfoList
                }
            } catch (e: Exception) {
                log.error(e) {
                    "Exception during get categories analytics." +
                            " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy"
                }
                emptyList()
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
            queryPeriod = conversionService.convert(datePeriod, QueryPeriod::class.java)!!,
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

    private suspend fun calculateCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
    ): CategoryAnalyticsInfo = coroutineScope {
        val daysBetween = ChronoUnit.DAYS.between(fromTime, toTime)
        val prevFromTime = fromTime.minusDays(daysBetween)
        val prevToTime = fromTime
        log.debug {
            "Calculate category analytics currentFromTime=$fromTime; currentToTime=$toTime;" +
                    " previousFromTime=$prevFromTime; previousToTime=$prevToTime"
        }
        val categoryAnalyticsTask = async {
            chCategoryRepository.getCategoryAnalyticsWithPrev(
                categoryId = categoryId,
                fromTime = fromTime,
                toTime = toTime,
                prevFromTime = prevFromTime,
                prevToTime = prevToTime,
            )
        }
        val categoryHierarchyTask = async {
            chCategoryRepository.getCategoryHierarchy(categoryId)!!
        }
        val categoryAnalytics = categoryAnalyticsTask.await()
        val chCategoryHierarchy = categoryHierarchyTask.await()

        CategoryAnalyticsInfo(
            category = Category(
                categoryId = categoryId,
                name = chCategoryHierarchy.name,
                parentId = chCategoryHierarchy.parentId,
                childrenIds = chCategoryHierarchy.childrenIds
            ),
            analytics = mapCategoryAnalytics(categoryAnalytics?.chCategoryAnalytics!!),
            analyticsPrevPeriod = mapCategoryAnalytics(categoryAnalytics.chCategoryAnalytics),
            analyticsDifference = mapCategoryAnalyticsDifference(
                categoryAnalytics.chCategoryAnalytics,
                categoryAnalytics.prevChCategoryAnalytics
            )
        )
    }

    private fun sortCategoryAnalytics(
        categoryAnalytics: List<CategoryAnalyticsInfo>,
        sortBy: SortBy
    ): List<CategoryAnalyticsInfo> {
        val comparators = sortBy.sortFields.map { sortField ->
            when (sortField.fieldName) {
                "order_amount" -> compareBy<CategoryAnalyticsInfo> { it.analytics.salesCount }
                "revenue" -> compareBy { it.analytics.revenue }
                "avg_bill" -> compareBy { it.analytics.averageBill }
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
        categoryAnalytics: ChCategoryAnalytics,
        prevCategoryAnalytics: ChCategoryAnalytics
    ): CategoryAnalyticsDifference {
        return CategoryAnalyticsDifference(
            revenuePercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.revenue,
                categoryAnalytics.revenue
            ).setScale(1, RoundingMode.DOWN),
            revenuePerProductPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.revenuePerProduct,
                categoryAnalytics.revenuePerProduct
            ).setScale(1, RoundingMode.DOWN),
            salesCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderAmount,
                categoryAnalytics.orderAmount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            productCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.productCount,
                categoryAnalytics.productCount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            sellerCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.sellerCount,
                categoryAnalytics.sellerCount
            ).toBigDecimal().setScale(1, RoundingMode.DOWN),
            averageBillPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.avgBill,
                categoryAnalytics.avgBill
            ).setScale(1, RoundingMode.DOWN),
            tstcPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderPerSeller,
                categoryAnalytics.orderPerSeller
            ).setScale(1, RoundingMode.DOWN),
            tstsPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderPerProduct,
                categoryAnalytics.orderPerProduct
            ).setScale(1, RoundingMode.DOWN)
        )
    }

    private fun mapCategoryAnalytics(categoryAnalytics: ChCategoryAnalytics): CategoryAnalytics {
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

}
