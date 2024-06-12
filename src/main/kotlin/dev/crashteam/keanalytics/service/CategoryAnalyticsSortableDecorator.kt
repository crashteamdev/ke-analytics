package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryHierarchy
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import dev.crashteam.keanalytics.service.model.CategoryAnalyticsCacheableWrapper
import dev.crashteam.keanalytics.service.model.CategoryAnalyticsInfo
import dev.crashteam.keanalytics.service.model.CategoryDailyAnalytics
import dev.crashteam.mp.base.DatePeriod
import dev.crashteam.mp.base.Filter
import dev.crashteam.mp.base.LimitOffsetPagination
import dev.crashteam.mp.base.Sort
import dev.crashteam.mp.external.analytics.category.ProductAnalytics
import org.springframework.stereotype.Service
import java.time.LocalDate

@Service
class CategoryAnalyticsSortableDecorator(
    private val categoryAnalyticsService: CategoryAnalyticsService
) {

    suspend fun getRootCategoryAnalytics(
        datePeriod: DatePeriod, sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo> {
        var categoryAnalytics = categoryAnalyticsService.getRootCategoryAnalytics(datePeriod).categoryAnalytics
            ?: return emptyList()
        categoryAnalytics = if (sortBy != null) {
            categoryAnalyticsService.sortCategoryAnalytics(categoryAnalytics, sortBy)
        } else {
            categoryAnalytics
        }
        return categoryAnalytics
    }

    suspend fun getCategoryAnalytics(
        categoryId: Long, datePeriod: DatePeriod, sortBy: SortBy? = null
    ): List<CategoryAnalyticsInfo> {
        var categoryAnalytics = categoryAnalyticsService.getCategoryAnalytics(categoryId, datePeriod).categoryAnalytics
            ?: return emptyList()
        categoryAnalytics = if (sortBy != null) {
            categoryAnalyticsService.sortCategoryAnalytics(categoryAnalytics, sortBy)
        } else {
            categoryAnalytics
        }
        return categoryAnalytics
    }

    fun getCategoryDailyAnalytics(
        categoryId: Long, fromTime: LocalDate, toTime: LocalDate
    ): List<CategoryDailyAnalytics> {
        return categoryAnalyticsService.getCategoryDailyAnalytics(categoryId, fromTime, toTime)
    }

    fun getCategoryProductsAnalytics(
        categoryId: Long,
        datePeriod: DatePeriod,
        filter: List<Filter>? = null,
        sort: List<Sort>? = null,
        page: LimitOffsetPagination
    ): List<ProductAnalytics> {
        return categoryAnalyticsService.getCategoryProductsAnalytics(categoryId, datePeriod, filter, sort, page)
    }

    fun getCategoryInfo(categoryId: Long): ChCategoryHierarchy? {
        return categoryAnalyticsService.getCategoryInfo(categoryId)
    }

}
