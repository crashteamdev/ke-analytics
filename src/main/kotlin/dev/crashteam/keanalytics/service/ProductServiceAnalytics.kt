package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.config.RedisConfig
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryOverallInfo
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesHistory
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductsSales
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class ProductServiceAnalytics(
    private val chProductRepository: CHProductRepository
) {

    @Cacheable(value = [RedisConfig.CATEGORY_OVERALL_INFO_CACHE], unless = "#result == null")
    fun getCategoryOverallAnalytics(
        categoryId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): ChCategoryOverallInfo? {
        return chProductRepository.getCategoryAnalytics(categoryId, fromTime, toTime)
    }

    fun getProductAnalytics(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductSalesHistory> {
        return chProductRepository.getProductSales(productId.toString(), skuId.toString(), fromTime, toTime)
    }

    fun getProductSalesAnalytics(
        productIds: List<Long>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductsSales> {
        val productIdList = productIds.map { it.toString() }
        return chProductRepository.getProductsSales(productIdList, fromTime, toTime)
    }
}
