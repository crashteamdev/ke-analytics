package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.math.MathUtils
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryAnalytics
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import dev.crashteam.keanalytics.service.model.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.temporal.ChronoUnit

private val log = KotlinLogging.logger {}

@Service
class CategoryAnalyticsService(
    private val chCategoryRepository: CHCategoryRepository
) {

    suspend fun getRootCategoryAnalytics(
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo>? {
        return coroutineScope {
            log.debug { "Get root categories analytics (Async)." +
                    "fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy" }
            val rootCategoryIds = chCategoryRepository.getDescendantCategories(0, 1)
            log.debug { "Root categories: $rootCategoryIds" }
            val categoryAnalyticsInfoList = rootCategoryIds?.map { rootCategoryId ->
                async {
                    calculateCategoryAnalytics(rootCategoryId, fromTime, toTime)
                }
            }?.awaitAll()
            log.debug { "Finish get root categories analytics (Async)." +
                    " fromTime=$fromTime; toTime=$toTime;" +
                    " sortBy=$sortBy; resultSize=${categoryAnalyticsInfoList?.size}" }
            categoryAnalyticsInfoList
        }
    }

    suspend fun getCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo>? {
        return coroutineScope {
            log.debug { "Get category analytics (Async)." +
                    " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime; sortBy=$sortBy" }
            val childrenCategoryIds = chCategoryRepository.getDescendantCategories(categoryId, 1)
            val categoryAnalyticsInfoList = childrenCategoryIds?.map { categoryId ->
                async {
                    calculateCategoryAnalytics(categoryId, fromTime, toTime)
                }
            }?.awaitAll()
            log.debug { "Finish get category analytics (Async)." +
                    " categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime;" +
                    " sortBy=$sortBy; resultSize=${categoryAnalyticsInfoList?.size}" }
            categoryAnalyticsInfoList
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

    private fun calculateCategoryAnalytics(
        categoryId: Long,
        fromTime: LocalDate,
        toTime: LocalDate,
        sortBy: SortBy? = null,
    ): CategoryAnalyticsInfo {
        val daysBetween = ChronoUnit.DAYS.between(fromTime, toTime)
        val prevFromTime = fromTime.minusDays(daysBetween)
        val prevToTime = fromTime
        log.debug { "Calculate category analytics currentFromTime=$fromTime; currentToTime=$toTime;" +
                " previousFromTime=$prevFromTime; previousToTime=$prevToTime" }
        val categoryAnalytics = chCategoryRepository.getCategoryAnalytics(
            categoryId = categoryId,
            fromTime = fromTime,
            toTime = toTime,
            sort = sortBy,
        )!!
        log.debug { "Calculated category analytics: $categoryAnalytics. categoryId=$categoryId; fromTime=$fromTime; toTime=$toTime" }
        val prevCategoryAnalytics = chCategoryRepository.getCategoryAnalytics(
            categoryId = categoryId,
            fromTime = prevFromTime,
            toTime = prevToTime,
            sort = sortBy
        )!!
        log.debug { "Calculated category analytics for previous period: $prevCategoryAnalytics." +
                " categoryId=$categoryId; fromTime=$prevFromTime; toTime=$prevToTime" }
        val categoryHierarchy = chCategoryRepository.getCategoryHierarchy(categoryId)!!
        log.debug { "Category hierarchy: $categoryHierarchy. categoryId=$categoryId" }
        return CategoryAnalyticsInfo(
            category = Category(
                categoryId = categoryId,
                name = categoryHierarchy.name,
                parentId = categoryHierarchy.parentId,
                childrenIds = categoryHierarchy.childrenIds
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
            ).setScale(1),
            revenuePerProductPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.revenuePerProduct,
                categoryAnalytics.revenuePerProduct
            ),
            salesCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderAmount,
                categoryAnalytics.orderAmount
            ).toBigDecimal().setScale(1),
            productCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.productCount,
                categoryAnalytics.productCount
            ).toBigDecimal().setScale(1),
            sellerCountPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.sellerCount,
                categoryAnalytics.sellerCount
            ).toBigDecimal().setScale(1),
            averageBillPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.avgBill,
                categoryAnalytics.avgBill
            ),
            tstcPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderPerSeller,
                categoryAnalytics.orderPerSeller
            ),
            tstsPercentage = MathUtils.percentageDifference(
                prevCategoryAnalytics.orderPerProduct,
                categoryAnalytics.orderPerProduct
            )
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
