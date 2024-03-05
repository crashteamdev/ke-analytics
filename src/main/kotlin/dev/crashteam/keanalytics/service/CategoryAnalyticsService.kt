package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.math.MathUtils
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalytics
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import dev.crashteam.keanalytics.service.model.*
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.math.RoundingMode
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

private val log = KotlinLogging.logger {}

@Service
class CategoryAnalyticsService(
    private val chCategoryRepository: CHCategoryRepository
) {

    @OptIn(ExperimentalTime::class)
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
                val (value, measureTimeMillis) = measureTimedValue {
                    val categoryAnalyticsInfoList = rootCategoryIds?.map { rootCategoryId ->
                        async {
                            val (value, measureTimeMillis) = measureTimedValue {
                                calculateCategoryAnalytics(rootCategoryId, fromTime, toTime)
                            }
                            log.debug { "Calculate category '$rootCategoryId' duration: $measureTimeMillis ms." }

                            value
                        }
                    }?.awaitAll()
                    categoryAnalyticsInfoList
                }
                log.debug { "Overall Calculate category. duration (ASYNC): $measureTimeMillis ms." }
                log.debug {
                    "Finish get root categories analytics (Async)." +
                            " fromTime=$fromTime; toTime=$toTime;" +
                            " sortBy=$sortBy; resultSize=${value?.size}"
                }
                value
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
                    "Finish get category analytics (Async)." +
                            " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime;" +
                            " sortBy=$sortBy; resultSize=${categoryAnalyticsInfoList?.size}"
                }
                categoryAnalyticsInfoList
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
                    revenue = categoryDailyAnalytics.revenue,
                    salesCount = categoryDailyAnalytics.orderAmount,
                    availableCount = categoryDailyAnalytics.availableAmount,
                    averageBill = categoryDailyAnalytics.averageBill
                )
            }
    }

    private suspend fun calculateCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null,
    ): CategoryAnalyticsInfo = coroutineScope {
        val daysBetween = ChronoUnit.DAYS.between(fromTime, toTime)
        val prevFromTime = fromTime.minusDays(daysBetween)
        val prevToTime = fromTime
        log.debug {
            "Calculate category analytics currentFromTime=$fromTime; currentToTime=$toTime;" +
                    " previousFromTime=$prevFromTime; previousToTime=$prevToTime"
        }
        val categoryAnalyticsTask = async {
            chCategoryRepository.getCategoryAnalytics(
                categoryId = categoryId,
                fromTime = fromTime,
                toTime = toTime,
                sort = sortBy,
            )!!
        }
        val prevCategoryAnalyticsTask = async {
            chCategoryRepository.getCategoryAnalytics(
                categoryId = categoryId,
                fromTime = prevFromTime,
                toTime = prevToTime,
                sort = sortBy
            )!!
        }
        val categoryHierarchyTask = async {
            chCategoryRepository.getCategoryHierarchy(categoryId)!!
        }
        val categoryAnalytics = categoryAnalyticsTask.await()
        val prevCategoryAnalytics = prevCategoryAnalyticsTask.await()
        val chCategoryHierarchy = categoryHierarchyTask.await()

        CategoryAnalyticsInfo(
            category = Category(
                categoryId = categoryId,
                name = chCategoryHierarchy.name,
                parentId = chCategoryHierarchy.parentId,
                childrenIds = chCategoryHierarchy.childrenIds
            ),
            analytics = mapCategoryAnalytics(categoryAnalytics),
            analyticsPrevPeriod = mapCategoryAnalytics(prevCategoryAnalytics),
            analyticsDifference = mapCategoryAnalyticsDifference(categoryAnalytics, prevCategoryAnalytics)
        )
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
            revenue = categoryAnalytics.revenue,
            revenuePerProduct = categoryAnalytics.revenuePerProduct,
            salesCount = categoryAnalytics.orderAmount,
            productCount = categoryAnalytics.productCount,
            sellerCount = categoryAnalytics.sellerCount,
            averageBill = categoryAnalytics.avgBill,
            tsts = categoryAnalytics.orderPerProduct,
            tstc = categoryAnalytics.orderPerSeller,
        )
    }

}
